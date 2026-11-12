#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Reference / test SDSP for DRIP Crowd Sourced RemoteID (draft-moskowitz-drip-crowd-sourced-rid-16).

EXPERIMENTAL, for testing Finders. A CoAP server (aiocoap) that:

- accepts CS-RID messages as COSE_Mac0 (pycose, HMAC 256/256, key per Finder chosen by the
  kid) or, with --allow-unauthenticated, as bare CBOR (cbor2);
- answers a Finder Registration with an SDSP Response (proxy status, update interval);
- decodes the forwarded ASTM F3411 B-RID messages and keeps a table of the UAs and Finders;
- puts an SDSP Heartbeat in the response to the first message of a Finder per
  --heartbeat-s (section 4.1: not one per message);
- answers 4.01 to unknown Finders, wrong MACs and Finders it has no registration of (the
  Finder then registers again);
- serves the state as JSON over HTTP: /finders, /uas, /messages, /stats.

Independent of the Finder code in the DroneScout gateway (other CBOR / COSE / CoAP
implementations), so it checks it. Standalone: needs only cbor2, aiocoap and pycose.

    python3 sdsp.py --finders finders.json [--port 5683] [--http-port 8088] [--log csrid.jsonl]

finders.json: {"<Finder ID>": {"key": "<hex>", "proxy_status": "forward"}, ...}
"""

import argparse
import asyncio
import datetime
import json
import math
import os
import signal
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import aiocoap
import aiocoap.resource
import cbor2
from pycose.algorithms import HMAC256
from pycose.headers import KID, Algorithm
from pycose.keys import SymmetricKey
from pycose.messages import CoseMessage, Mac0Message

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from brid import decode_brid                     # noqa: E402

APPLICATION = 'DRIP-CSRID'
LABELS = {'message_info': 0, 'proxy_message-type': 1, 'finder_registration': 2, 'sdsp_response': 3,
          'location_update': 4, 'sdsp_heartbeat': 5}
TYPE_NAMES = {0: 'info', 1: 'brid', 2: 'registration', 3: 'sdsp_response', 4: 'location', 5: 'heartbeat'}
PROXY_STATUS = {'forward': 0, 'reverse': 1, 'bi-directional': 2, 'bi': 2}
CF_CBOR, CF_COSE_MAC0 = 60, 97
RADIO = {0: 'EFL', 1: 'VLF', 2: 'LF', 3: 'MF', 4: 'HF', 5: 'HF', 6: 'VHF', 7: 'UHF', 8: 'SHF', 9: 'EHF'}


class Reject(Exception):
    def __init__(self, code, text):
        super().__init__(text)
        self.code = code


# --- state ------------------------------------------------------------------------------------

def _jsonable(v):
    if isinstance(v, bytes):
        return v.hex()
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    if isinstance(v, cbor2.CBORTag):
        return {'tag': v.tag, 'value': _jsonable(v.value)}
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


class SDSP:
    def __init__(self, sdsp_id, finders, update_interval=300, heartbeat_s=3600, max_skew_s=600,
                 allow_unauthenticated=False, log_path=None, quiet=False, clock=time.time):
        self.sdsp_id = sdsp_id
        self.finders = finders                    # rid -> {"key": bytes, "proxy_status": int}
        self.update_interval = update_interval
        self.heartbeat_s = heartbeat_s
        self.max_skew_s = max_skew_s
        self.allow_unauthenticated = allow_unauthenticated
        self.quiet = quiet
        self.clock = clock
        self.mac = ':'.join('%02x' % ((uuid.getnode() >> s) & 0xff) for s in range(40, -1, -8))
        self.lock = threading.Lock()
        self.registered = {}                       # rid -> finder state
        self.uas = {}                              # broadcast MAC -> UA state
        self.messages = []                         # last 200 decoded messages
        self.stats = {'received': 0, 'rejected': 0, 'heartbeats sent': 0}
        self.log_file = open(log_path, 'a', buffering=1) if log_path else None

    def out(self, text):
        if not self.quiet:
            print(text)

    # --- messages from the SDSP ---

    def _object(self, label, members):
        body = {'message_type': LABELS[label], 'mac_address': bytes.fromhex(self.mac.replace(':', ''))}
        body.update(members)
        return {'application': APPLICATION, 'version': 1, label: body}

    def _seal(self, obj, key):
        payload = cbor2.dumps(obj, datetime_as_timestamp=False)
        if key is None:
            return payload, CF_CBOR
        msg = Mac0Message(phdr={Algorithm: HMAC256}, uhdr={KID: self.sdsp_id.encode()}, payload=payload)
        msg.key = SymmetricKey(k=key)
        return msg.encode(), CF_COSE_MAC0

    def response(self, rid, key):
        return self._seal(self._object('sdsp_response', {
            'sdsp_id': self.sdsp_id, 'id': rid, 'proxy_status_type': self.finders.get(rid, {}).get('proxy_status', 0),
            'update_interval': self.update_interval}), key)

    def heartbeat(self, key):
        now = datetime.datetime.fromtimestamp(self.clock(), datetime.timezone.utc)
        return self._seal(self._object('sdsp_heartbeat', {'sdsp_id': self.sdsp_id, 'timestamp': now}), key)

    # --- messages to the SDSP ---

    def open(self, payload, content_format):
        """bytes -> (rid used for the key, key, decoded CSRID object)."""
        if content_format == CF_CBOR or (content_format is None and payload[:1] != b'\xd1'):
            if not self.allow_unauthenticated:
                raise Reject(aiocoap.UNAUTHORIZED, "unauthenticated message (no COSE_Mac0)")
            return None, None, cbor2.loads(payload)
        try:
            msg = CoseMessage.decode(payload)
        except Exception as e:
            raise Reject(aiocoap.BAD_REQUEST, "not a COSE message: %s" % e)
        if not isinstance(msg, Mac0Message):
            raise Reject(aiocoap.BAD_REQUEST, "COSE %s, expected COSE_Mac0" % type(msg).__name__)
        kid = msg.get_attr(KID)
        rid = kid.decode('utf-8', 'replace') if isinstance(kid, bytes) else None
        finder = self.finders.get(rid)
        if finder is None:
            raise Reject(aiocoap.UNAUTHORIZED, "unknown Finder %r" % rid)
        msg.key = SymmetricKey(k=finder['key'])
        try:
            ok = msg.verify_tag()
        except Exception as e:
            raise Reject(aiocoap.UNAUTHORIZED, "MAC check failed for %s: %s" % (rid, e))
        if not ok:
            raise Reject(aiocoap.UNAUTHORIZED, "wrong MAC from %s" % rid)
        return rid, finder['key'], cbor2.loads(msg.payload)

    def handle(self, payload, content_format, remote='?'):
        """One CoAP request payload -> (code, response payload, content format)."""
        with self.lock:
            self.stats['received'] += 1
        try:
            return self._handle(payload, content_format, remote)
        except Reject as e:
            with self.lock:
                self.stats['rejected'] += 1
            self.out('REJECT %s from %s: %s' % (e.code, remote, e))
            return e.code, b'', None
        except Exception as e:
            with self.lock:
                self.stats['rejected'] += 1
            self.out('REJECT 4.00 from %s: %s' % (remote, e))
            return aiocoap.BAD_REQUEST, b'', None

    def _handle(self, payload, content_format, remote):
        kid_rid, key, obj = self.open(payload, content_format)
        if not isinstance(obj, dict) or obj.get('application') != APPLICATION:
            raise Reject(aiocoap.BAD_REQUEST, "not a DRIP-CSRID object")
        found = [(LABELS[k], v) for k, v in obj.items() if k in LABELS]
        if len(found) != 1:
            raise Reject(aiocoap.BAD_REQUEST, "%d messages in the object" % len(found))
        mtype, m = found[0]
        if not isinstance(m, dict) or m.get('message_type') != mtype:
            raise Reject(aiocoap.BAD_REQUEST, "message_type doesn't match the label")
        rid = m.get('id')
        if not isinstance(rid, str):
            raise Reject(aiocoap.BAD_REQUEST, "no Finder id")
        if kid_rid is not None and rid != kid_rid:
            raise Reject(aiocoap.UNAUTHORIZED, "id %r doesn't match the key ID %r" % (rid, kid_rid))
        ts = m.get('timestamp')
        if not isinstance(ts, datetime.datetime):
            raise Reject(aiocoap.BAD_REQUEST, "timestamp is not a tdate")
        t = ts.timestamp()
        if abs(t - self.clock()) > self.max_skew_s:
            raise Reject(aiocoap.BAD_REQUEST, "timestamp %s more than %d s off" % (ts.isoformat(), self.max_skew_s))
        gps = m.get('gps')
        if not (isinstance(gps, list) and len(gps) == 3 and all(isinstance(x, (int, float)) for x in gps)):
            raise Reject(aiocoap.BAD_REQUEST, "gps is not [lat, long, alt]")
        now = self.clock()
        record = {'received': now, 'from': remote, 'type': TYPE_NAMES.get(mtype, mtype), 'finder': rid,
                  'authenticated': key is not None, 'message': _jsonable(m)}

        with self.lock:
            finder = self.registered.get(rid)
            if mtype == 2:                                         # Finder Registration
                finder = self.registered[rid] = {
                    'id': rid, 'registered': now, 'gps': gps, 'last_seen': now, 'messages': 0,
                    'mac_address': _jsonable(m.get('mac_address')), 'last_heartbeat': now, 'address': remote,
                    'authenticated': key is not None}
                answer = self.response(rid, key)
                self.out('REGISTER %s at %.6f, %.6f, %.0f m from %s -> SDSP Response' % (rid, gps[0], gps[1], gps[2], remote))
            elif finder is None:
                raise Reject(aiocoap.UNAUTHORIZED, "Finder %s isn't registered" % rid)
            elif mtype in (1, 4):
                if mtype == 1:
                    self._brid(rid, m, t, gps, record)
                else:
                    self.out('LOCATION %s at %.6f, %.6f, %.0f m' % (rid, gps[0], gps[1], gps[2]))
                finder['gps'] = gps
                answer = None
                if now - finder['last_heartbeat'] >= self.heartbeat_s:
                    finder['last_heartbeat'] = now
                    self.stats['heartbeats sent'] += 1
                    answer = self.heartbeat(key)
            else:
                raise Reject(aiocoap.BAD_REQUEST, "a Finder can't send %s messages" % TYPE_NAMES.get(mtype))
            finder['last_seen'] = now
            finder['address'] = remote
            finder['messages'] += 1
            self.messages = (self.messages + [record])[-200:]
        if self.log_file:
            self.log_file.write(json.dumps(record) + '\n')
        if answer is None:
            return aiocoap.CHANGED, b'', None
        return aiocoap.CHANGED, answer[0], answer[1]

    def _brid(self, rid, m, t, gps, record):
        brid = m.get('broadcast_message')
        mac = m.get('broadcast_mac_address')
        if not isinstance(brid, bytes) or not isinstance(mac, bytes):
            raise Reject(aiocoap.BAD_REQUEST, "broadcast_message / broadcast_mac_address not byte strings")
        try:
            decoded = decode_brid(brid)
        except ValueError as e:
            raise Reject(aiocoap.BAD_REQUEST, "B-RID: %s" % e)
        record['brid'] = decoded
        key = mac.hex(':')
        ua = self.uas.setdefault(key, {'broadcast_mac_address': key, 'first_seen': t, 'messages': 0, 'finders': {}})
        ua['last_seen'] = t
        ua['messages'] += 1
        ua['radio_type'] = RADIO.get(m.get('radio_type'), m.get('radio_type'))
        ua['finders'][rid] = {'last_seen': t, 'gps': gps}
        for d in decoded:
            kind = d['type']
            if kind == 'basic_id':
                ua.setdefault('basic_id', {})[str(d['id_type'])] = d
            elif kind == 'auth':
                ua.setdefault('auth_pages', {})[str(d['page'])] = d
            else:
                ua[kind] = d
        loc = ua.get('location', {})
        ids = ', '.join(b['uas_id'] for b in ua.get('basic_id', {}).values()) or key
        self.out('B-RID %s via %s (%s): %s %s, %s m, %d messages'
                 % (ids, rid, ua['radio_type'], loc.get('latitude'), loc.get('longitude'),
                    loc.get('altitude_geo'), len(decoded)))

    def snapshot(self, what):
        with self.lock:
            if what == 'finders':
                return _jsonable(self.registered)
            if what == 'uas':
                return _jsonable(self.uas)
            if what == 'messages':
                return list(self.messages)
            return dict(self.stats, finders=len(self.registered), uas=len(self.uas), sdsp_id=self.sdsp_id)


# --- servers ----------------------------------------------------------------------------------

class CSRIDResource(aiocoap.resource.Resource):
    def __init__(self, sdsp):
        super().__init__()
        self.sdsp = sdsp

    async def render_post(self, request):
        remote = str(request.remote.hostinfo) if request.remote else '?'
        code, payload, cf = self.sdsp.handle(request.payload, request.opt.content_format, remote)
        response = aiocoap.Message(code=code, payload=payload)
        if cf is not None:
            response.opt.content_format = cf
        return response


def http_server(sdsp, host, port):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            what = self.path.strip('/').split('?')[0] or 'stats'
            if what not in ('finders', 'uas', 'messages', 'stats'):
                self.send_error(404, 'use /finders, /uas, /messages or /stats')
                return
            data = json.dumps(sdsp.snapshot(what), indent=1).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    srv = ThreadingHTTPServer((host, port), Handler)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def load_finders(path):
    with open(path) as f:
        data = json.load(f)
    finders = {}
    for rid, f in data.items():
        if rid.startswith('_'):
            continue
        status = f.get('proxy_status', 'forward')
        finders[rid] = {'key': bytes.fromhex(f['key']),
                        'proxy_status': PROXY_STATUS[status] if isinstance(status, str) else int(status)}
    return finders


async def serve(sdsp, host, port, path):
    root = aiocoap.resource.Site()
    root.add_resource([p for p in path.strip('/').split('/') if p], CSRIDResource(sdsp))
    context = await aiocoap.Context.create_server_context(root, bind=(host, port))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    await context.shutdown()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--finders', required=True, help='JSON file with the Finder IDs and keys')
    p.add_argument('--sdsp-id', default='dronescout-test-sdsp')
    p.add_argument('--host', default='::', help='CoAP bind address')
    p.add_argument('--port', type=int, default=5683)
    p.add_argument('--path', default='csrid')
    p.add_argument('--http-host', default='0.0.0.0')
    p.add_argument('--http-port', type=int, default=8088, help='JSON view; 0: off')
    p.add_argument('--update-interval', type=int, default=300, help='Finder location update interval (s)')
    p.add_argument('--heartbeat-s', type=int, default=3600, help='at most one heartbeat per Finder per this time')
    p.add_argument('--max-skew-s', type=int, default=600, help='reject messages with timestamps further off')
    p.add_argument('--allow-unauthenticated', action='store_true', help='also accept bare CBOR without a MAC')
    p.add_argument('--log', help='append every accepted message as JSON lines to this file')
    a = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)
    sdsp = SDSP(a.sdsp_id, load_finders(a.finders), a.update_interval, a.heartbeat_s, a.max_skew_s,
                a.allow_unauthenticated, a.log)
    if a.http_port:
        http_server(sdsp, a.http_host, a.http_port)
    print('SDSP %s: CoAP on [%s]:%d/%s, %d Finders, JSON view on http port %s (EXPERIMENTAL, draft -16)'
          % (a.sdsp_id, a.host, a.port, a.path, len(sdsp.finders), a.http_port or 'off'))
    asyncio.run(serve(sdsp, a.host, a.port, a.path))
    print(json.dumps(sdsp.snapshot('stats')))


if __name__ == '__main__':
    main()

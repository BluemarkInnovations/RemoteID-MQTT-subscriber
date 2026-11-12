# (c) Bluemark Innovations BV
# MIT license
"""CS-RID Finder: forwards the received Broadcast RemoteID to an SDSP over CoAP.

draft-moskowitz-drip-crowd-sourced-rid-16 (experimental). The gateway is one Finder; the
DroneScout sensors are its receivers:

1. Finder Registration (CoAP CON POST, retransmitted, then retried every
   registration_retry_s) with the Finder position. The SDSP answers with an SDSP Response:
   its ID, the proxy status and the location update interval.
2. B-RID Forwarding (CoAP NON POST) for every RemoteID message, at most one per drone per
   min_interval_s: the B-RID messages re-encoded as an ASTM Message Pack, the drone's MAC
   address, the receive time and the position of the sensor that received it.
3. Finder Location (NON) when no message with the Finder position went out for the
   update interval (+ < 1 s random slew, section 6.5).

The SDSP may put an SDSP Response or an SDSP Heartbeat in any response; it may also send
them to the Finder's port as a CoAP request. A 4.01 / 4.03 answer makes the Finder
register again. Every message is a COSE_Mac0 with the Finder's key (HMAC-SHA256).
"""

import os
import queue
import random
import select
import socket
import threading
import time
import urllib.parse
import uuid

from modules import decode
from modules import exporters
from integrations.csrid import astm
from integrations.csrid import coap
from integrations.csrid import csrid_map as cm

NON_LIFETIME_S = 10.0           # how long a response to a NON request is still matched


def parse_sdsp_url(url):
    """'coap://host[:port][/path]' -> (host, port, path)."""
    u = urllib.parse.urlsplit(str(url))
    if u.scheme != 'coap':
        raise ValueError("SDSP URL must be coap://host[:port]/path (coaps / DTLS is not supported): %r" % url)
    if not u.hostname:
        raise ValueError("no host in the SDSP URL %r" % url)
    return u.hostname, u.port or 5683, u.path.strip('/') or 'csrid'


def default_finder_mac():
    node = uuid.getnode()
    return ':'.join('%02x' % ((node >> s) & 0xff) for s in range(40, -1, -8))


class _Pending:
    __slots__ = ('kind', 'mid', 'token', 'data', 'confirmable', 'sent', 'retries', 'timeout', 'acked')

    def __init__(self, kind, mid, token, data, confirmable, now):
        self.kind = kind
        self.mid = mid
        self.token = token
        self.data = data
        self.confirmable = confirmable
        self.sent = now
        self.retries = 0
        self.timeout = coap.ACK_TIMEOUT
        self.acked = False


class CSRIDFinder(exporters.Exporter):
    name = "csrid"

    def __init__(self, sdsp_url, rid, key=None, finder_mac=None, position=None, position_sensor=None,
                 min_interval_s=1.0, registration_retry_s=30.0, max_queue=1000, log=print,
                 clock=time.time, bind=('', 0), start=True):
        self.host, self.port, self.path = parse_sdsp_url(sdsp_url)
        if not rid:
            raise ValueError("no Finder ID (csrid_finder_id)")
        self.rid = str(rid)
        self.key = bytes.fromhex(key) if isinstance(key, str) else key
        if self.key is not None and len(self.key) < 16:
            raise ValueError("the Finder key must be at least 16 bytes (32 hex digits)")
        self.finder_mac = finder_mac or default_finder_mac()
        self.position = tuple(position) if position else None        # (lat, lon, altitude MSL m)
        self.position_sensor = position_sensor
        self.min_interval_s = min_interval_s
        self.registration_retry_s = registration_retry_s
        self.log = log
        self._clock = clock

        self.registered = False
        self.sdsp_id = None
        self.proxy_status = cm.PROXY_FORWARD
        self.update_interval_s = cm.DEFAULT_UPDATE_INTERVAL_S
        self.last_heartbeat = None
        self.sent = {}                       # message type name -> count
        self.responses = 0
        self.heartbeats = 0
        self.errors = 0
        self.dropped = {'not registered': 0, 'suspended': 0, 'no position': 0, 'queue full': 0, 'rate': 0}
        self._sensor_positions = {}
        self._last_forward = {}              # drone key -> time
        self._last_gps_sent = None
        self._next_location = None
        self._next_registration = 0.0
        self._registering = False
        self._warned = set()

        self._queue = queue.Queue(max_queue)
        self._pending = {}                   # token -> _Pending
        self._mid = random.randrange(0x10000)
        self._addr = None
        self._closed = False
        self._lock = threading.Lock()
        self.sock = None
        self._bind = bind
        self._thread = None
        if start:
            self._thread = threading.Thread(target=self._loop, name='csrid', daemon=True)
            self._thread.start()

    # --- exporter callbacks ---------------------------------------------------------------------

    def on_location(self, location_json):
        lat, lon = location_json.get('latitude'), location_json.get('longitude')
        if lat is None or lon is None:
            return
        with self._lock:
            self._sensor_positions[str(location_json.get('sensor ID'))] = (lat, lon, location_json.get('altitude MSL'))

    def on_remote_id(self, msg):
        if not self.registered:
            self.dropped['not registered'] += 1
            return
        if not cm.proxying(self.proxy_status):
            self.dropped['suspended'] += 1
            return
        position = self._sensor_position(msg.sensor_id) or self.finder_position()
        if position is None:
            self.dropped['no position'] += 1
            self._warn_once('position', "CS-RID: no position of sensor %s yet (needs its location message, or "
                            "csrid_position): B-RID not forwarded" % msg.sensor_id)
            return
        now = self._clock()
        key = msg.mac_address or decode.drone_id(msg)
        with self._lock:
            last = self._last_forward.get(key)
            if last is not None and now - last < self.min_interval_s:
                self.dropped['rate'] += 1
                return
            self._last_forward[key] = now
            if len(self._last_forward) > 10000:
                self._last_forward = {k: v for k, v in self._last_forward.items() if now - v < 60}
        t = msg.timestamp_ms / 1000.0 if msg.timestamp_ms else now
        radio = cm.radio_type(msg.type, msg.channel)
        for pack in astm.message_packs(msg.uas):
            obj = cm.brid_forwarding(self.rid, self.finder_mac, t, position, radio, msg.mac_address, pack)
            self._put(cm.BRID_FORWARDING, cm.encode(obj, self.key, self.rid))

    # --- state ------------------------------------------------------------------------------------

    def _sensor_position(self, sensor_id):
        with self._lock:
            return self._sensor_positions.get(str(sensor_id))

    def finder_position(self):
        """The Finder's own position: fixed, else that of position_sensor, else of any sensor."""
        if self.position is not None:
            return self.position
        with self._lock:
            if self.position_sensor is not None:
                return self._sensor_positions.get(str(self.position_sensor))
            return next(iter(self._sensor_positions.values()), None)

    def _warn_once(self, what, text):
        if what not in self._warned:
            self._warned.add(what)
            self.log(text)

    def _put(self, mtype, data):
        try:
            self._queue.put_nowait((mtype, data))
        except queue.Full:
            self.dropped['queue full'] += 1

    def _count(self, mtype):
        name = cm.TYPE_NAMES[mtype]
        self.sent[name] = self.sent.get(name, 0) + 1

    # --- CoAP -----------------------------------------------------------------------------------

    def _open(self):
        if self.sock is not None:
            return True
        try:
            family, _, _, _, addr = socket.getaddrinfo(self.host, self.port, 0, socket.SOCK_DGRAM)[0]
        except OSError as e:
            self._warn_once('resolve', "CS-RID: can't resolve the SDSP %s: %s" % (self.host, e))
            return False
        self.sock = socket.socket(family, socket.SOCK_DGRAM)
        if self._bind and family == socket.AF_INET:
            self.sock.bind(self._bind)
        self._addr = addr
        return True

    def _next_mid(self):
        self._mid = (self._mid + 1) & 0xFFFF
        return self._mid

    def _send_request(self, kind, payload, confirmable):
        token = os.urandom(4)
        options = coap.path_options(self.path) + [
            (coap.OPT_CONTENT_FORMAT, coap.uint_option(coap.CF_CBOR if self.key is None else coap.CF_COSE_MAC0))]
        msg = coap.Message(coap.CON if confirmable else coap.NON, coap.POST, self._next_mid(), token, options, payload)
        data = coap.encode(msg)
        p = _Pending(kind, msg.mid, token, data, confirmable, time.monotonic())
        self._pending[token] = p
        self._sendto(data)
        self._count(kind)
        return p

    def _sendto(self, data):
        try:
            self.sock.sendto(data, self._addr)
        except OSError as e:
            self.errors += 1
            if self.errors in (1, 10, 100) or self.errors % 1000 == 0:
                self.log("CS-RID: sending to the SDSP %s:%d failed: %s" % (self.host, self.port, e))

    def register(self):
        position = self.finder_position()
        if position is None:
            self._warn_once('register', "CS-RID: no Finder position yet (csrid_position / csrid_position_sensor): "
                            "waiting before registering")
            self._next_registration = time.monotonic() + 1.0
            return
        obj = cm.finder_registration(self.rid, self.finder_mac, self._clock(), position)
        self._registering = True
        self._send_request(cm.FINDER_REGISTRATION, cm.encode(obj, self.key, self.rid), confirmable=True)
        self._last_gps_sent = time.monotonic()

    def _send_location(self):
        position = self.finder_position()
        if position is None:
            return
        obj = cm.finder_location(self.rid, self.finder_mac, self._clock(), position)
        self._send_request(cm.FINDER_LOCATION, cm.encode(obj, self.key, self.rid), confirmable=False)
        self._last_gps_sent = time.monotonic()

    def _schedule_location(self):
        base = self._last_gps_sent if self._last_gps_sent is not None else time.monotonic()
        self._next_location = base + self.update_interval_s + random.random()

    def _registration_failed(self, why):
        self._registering = False
        self._next_registration = time.monotonic() + self.registration_retry_s
        self.log("CS-RID: registration at %s:%d failed: %s; retry in %d s"
                 % (self.host, self.port, why, self.registration_retry_s))

    def _receive(self, data, addr):
        try:
            msg = coap.decode(data)
        except ValueError:
            self.errors += 1
            return
        if msg.code in (coap.POST, 3) and msg.mtype in (coap.CON, coap.NON):    # SDSP-initiated (POST / PUT)
            ok = self._handle_payload(msg.payload, None)
            reply = coap.Message(coap.ACK if msg.mtype == coap.CON else coap.NON,
                                 coap.CHANGED if ok else coap.BAD_REQUEST,
                                 msg.mid if msg.mtype == coap.CON else self._next_mid(), msg.token)
            self._sendto_addr(coap.encode(reply), addr)
            return
        if msg.mtype == coap.RST:
            p = next((p for p in self._pending.values() if p.mid == msg.mid), None)
            if p is not None:
                del self._pending[p.token]
                if p.kind == cm.FINDER_REGISTRATION:
                    self._registration_failed("reset by the SDSP")
            return
        if msg.mtype == coap.ACK and msg.code == coap.EMPTY:                  # separate response follows
            p = next((p for p in self._pending.values() if p.mid == msg.mid), None)
            if p is not None:
                p.acked = True
            return
        p = self._pending.pop(msg.token, None)
        if msg.mtype == coap.CON:
            self._sendto_addr(coap.encode(coap.Message(coap.ACK, coap.EMPTY, msg.mid)), addr)
        if p is None:
            return
        self.responses += 1
        if coap.is_success(msg.code):
            handled = self._handle_payload(msg.payload, p.kind) if msg.payload else False
            if p.kind == cm.FINDER_REGISTRATION and not self.registered:
                self._registered(None if handled else "no SDSP Response, default update interval")
        elif msg.code in (coap.UNAUTHORIZED, coap.FORBIDDEN):
            if p.kind == cm.FINDER_REGISTRATION:
                self._registration_failed("SDSP answered %s (unknown Finder ID or wrong key?)" % coap.code_text(msg.code))
            elif self.registered:
                self.registered = False
                self._next_registration = 0.0
                self.log("CS-RID: SDSP answered %s, registering again" % coap.code_text(msg.code))
        else:
            self.errors += 1
            if p.kind == cm.FINDER_REGISTRATION:
                self._registration_failed("SDSP answered %s" % coap.code_text(msg.code))
            elif self.errors in (1, 10, 100) or self.errors % 1000 == 0:
                self.log("CS-RID: SDSP answered %s to a %s message" % (coap.code_text(msg.code), cm.TYPE_NAMES[p.kind]))

    def _sendto_addr(self, data, addr):
        try:
            self.sock.sendto(data, addr)
        except OSError:
            self.errors += 1

    def _registered(self, note=None):
        self.registered = True
        self._registering = False
        self._schedule_location()
        self.log("CS-RID: registered at SDSP %s (%s:%d), proxy status %s, location update every %d s%s"
                 % (self.sdsp_id or '?', self.host, self.port, cm.PROXY_STATUS_NAMES.get(self.proxy_status, '?'),
                    self.update_interval_s, ' (%s)' % note if note else ''))

    def _handle_payload(self, payload, request_kind):
        """An SDSP Response / Heartbeat / info from the SDSP. Returns True when understood."""
        try:
            mtype, m = cm.decode(payload, self.key)
        except (ValueError, KeyError) as e:
            self.errors += 1
            self.log("CS-RID: bad message from the SDSP: %s" % e)
            return False
        if mtype == cm.SDSP_RESPONSE:
            if m.get('id') != self.rid:
                self.log("CS-RID: SDSP Response for another Finder (%r), ignored" % m.get('id'))
                return False
            changed = (m.get('proxy_status_type'), m.get('update_interval')) != (self.proxy_status, self.update_interval_s)
            self.sdsp_id = m.get('sdsp_id', self.sdsp_id)
            if m.get('proxy_status_type') in cm.PROXY_STATUS_NAMES:
                self.proxy_status = m['proxy_status_type']
            if isinstance(m.get('update_interval'), int) and m['update_interval'] > 0:
                self.update_interval_s = m['update_interval']
            if not self.registered:
                self._registered()
            elif changed:
                self._schedule_location()
                self.log("CS-RID: SDSP %s: proxy status %s, location update every %d s"
                         % (self.sdsp_id, cm.PROXY_STATUS_NAMES[self.proxy_status], self.update_interval_s))
            return True
        if mtype == cm.SDSP_HEARTBEAT:
            self.heartbeats += 1
            self.last_heartbeat = self._clock()
            self.sdsp_id = m.get('sdsp_id', self.sdsp_id)
            return True
        if mtype == cm.RESERVED:
            self.log("CS-RID: SDSP info: %s" % m.get('message_content'))
            return True
        self.log("CS-RID: unexpected %s message from the SDSP" % cm.TYPE_NAMES[mtype])
        return False

    def _timers(self):
        now = time.monotonic()
        for p in list(self._pending.values()):
            if p.confirmable and not p.acked:
                if now - p.sent >= p.timeout:
                    if p.retries >= coap.MAX_RETRANSMIT:
                        del self._pending[p.token]
                        if p.kind == cm.FINDER_REGISTRATION:
                            self._registration_failed("no answer")
                        continue
                    p.retries += 1
                    p.sent = now
                    p.timeout *= 2
                    self._sendto(p.data)
            elif now - p.sent > (NON_LIFETIME_S if not p.confirmable else 60.0):
                del self._pending[p.token]
                if p.kind == cm.FINDER_REGISTRATION:
                    self._registration_failed("no response after the ACK")
        if not self.registered:
            if not self._registering and now >= self._next_registration:
                self.register()
        elif self._next_location is not None:
            if self._last_gps_sent is not None and self._last_gps_sent + self.update_interval_s > self._next_location:
                self._schedule_location()           # B-RID messages carried the position meanwhile
            elif now >= self._next_location:
                self._send_location()
                self._schedule_location()

    def poll(self, timeout=0.1):
        """One iteration of the worker: receive, timers, send the queue."""
        if not self._open():
            time.sleep(timeout)
            return
        r, _, _ = select.select([self.sock], [], [], timeout)
        if r:
            for _ in range(100):
                try:
                    data, addr = self.sock.recvfrom(2048)
                except (BlockingIOError, InterruptedError):
                    break
                except OSError:
                    break
                self._receive(data, addr)
                if not select.select([self.sock], [], [], 0)[0]:
                    break
        self._timers()
        for _ in range(200):
            try:
                mtype, data = self._queue.get_nowait()
            except queue.Empty:
                break
            if not self.registered:
                self.dropped['not registered'] += 1
                continue
            self._send_request(mtype, data, confirmable=False)
            self._last_gps_sent = time.monotonic()

    def _loop(self):
        while not self._closed:
            try:
                self.poll()
            except Exception as e:                  # never let the thread die
                self.errors += 1
                self.log("CS-RID: error: %r" % e)
                time.sleep(1)

    def stats(self):
        sent = ', '.join('%d %s' % (n, k) for k, n in sorted(self.sent.items())) or 'nothing'
        dropped = ', '.join('%d %s' % (n, k) for k, n in self.dropped.items() if n)
        return "%s; sent %s; %d responses, %d heartbeats, %d errors%s" % (
            'registered at %s' % self.sdsp_id if self.registered else 'not registered', sent, self.responses,
            self.heartbeats, self.errors, '; not forwarded: ' + dropped if dropped else '')

    def close(self):
        self._closed = True
        if self._thread is not None:
            self._thread.join(timeout=2)
        if self.sock is not None:
            self.sock.close()


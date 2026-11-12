#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Minimal stand-in for a TAK Server, to test tak_gateway.py without a real one.

Like TAK Server's streaming input it:
- accepts TLS connections and requires a client certificate signed by its CA
  (or plain TCP with --tcp),
- reads CoT events from the stream and prints them,
- answers a ping (t-x-c-t) with t-x-c-t-r,
- keeps presence events (clients announcing themselves, with <takv>) apart in .contacts,
- passes every other event on to the other connected clients,
- sends other traffic to the client every few seconds, like the positions of
  other TAK users, which a client has to read.

It does not store or forward anything. For testing only.

    integrations/tak/tools/make_test_certs.sh
    python3 integrations/tak/tools/mock_tak_server.py --certs test_certs
    (tak_config.py: tak_url = 'tls://localhost:8089', certificates from test_certs)
"""

import argparse
import socket
import ssl
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone


def _cot_time(t):
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def _other_user_event():
    now = time.time()
    return ('<event version="2.0" uid="ANDROID-mock-user" type="a-f-G-U-C" how="h-e" time="%s" start="%s" '
            'stale="%s"><point lat="52.0" lon="6.0" hae="10" ce="10" le="10"/><detail>'
            '<contact callsign="MOCK-USER"/></detail></event>'
            % (_cot_time(now), _cot_time(now), _cot_time(now + 60))).encode()


def _pong_event():
    now = time.time()
    return ('<event version="2.0" uid="takPong" type="t-x-c-t-r" how="h-g-i-g-o" time="%s" start="%s" '
            'stale="%s"><point lat="0" lon="0" hae="0" ce="9999999" le="9999999"/><detail/></event>'
            % (_cot_time(now), _cot_time(now), _cot_time(now + 20))).encode()


class MockTakServer:
    def __init__(self, host='127.0.0.1', port=8089, certfile=None, keyfile=None, cafile=None,
                 use_tls=True, chatter_interval_s=5.0, on_event=None):
        self.events = []           # (client certificate CN or address, event element)
        self.contacts = []         # presence events (with <takv>) clients announce themselves with
        self.client_names = []
        self.on_event = on_event
        self.chatter_interval_s = chatter_interval_s
        self._ctx = None
        if use_tls:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(certfile, keyfile)
            ctx.load_verify_locations(cafile)
            ctx.verify_mode = ssl.CERT_REQUIRED       # like TAK Server: no client certificate, no connection
            self._ctx = ctx
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((host, port))
        self._listener.listen()
        self.port = self._listener.getsockname()[1]
        self._clients = []
        self._lock = threading.Lock()
        self._stopped = False

    def start(self):
        threading.Thread(target=self._accept_loop, name='mock-tak-accept', daemon=True).start()
        return self

    def _accept_loop(self):
        while not self._stopped:
            try:
                raw, addr = self._listener.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(raw, addr), daemon=True).start()

    def _serve(self, raw, addr):
        try:
            if self._ctx is not None:
                conn = self._ctx.wrap_socket(raw, server_side=True)
                cert = conn.getpeercert()
                name = dict(x[0] for x in cert['subject']).get('commonName', str(addr))
            else:
                conn, name = raw, '%s:%d' % addr
        except (ssl.SSLError, OSError) as e:
            print("mock TAK: refused %s: %s" % (addr[0], e))
            raw.close()
            return
        print("mock TAK: %s connected" % name)
        with self._lock:
            self._clients.append(conn)
            self.client_names.append(name)
        if self.chatter_interval_s:
            threading.Thread(target=self._chatter, args=(conn,), daemon=True).start()
        buf = b''
        try:
            while True:
                data = conn.recv(65536)
                if not data:
                    break
                buf += data
                while b'</event>' in buf:
                    end = buf.index(b'</event>') + len(b'</event>')
                    start = buf.find(b'<event')
                    raw_event, buf = buf[start:end], buf[end:]
                    self._handle(conn, name, raw_event)
        except (OSError, ssl.SSLError):
            pass
        print("mock TAK: %s disconnected" % name)
        with self._lock:
            if conn in self._clients:
                self._clients.remove(conn)
        conn.close()

    def _handle(self, conn, name, raw_event):
        try:
            event = ET.fromstring(raw_event)
        except ET.ParseError as e:
            print("mock TAK: %s sent invalid XML: %s" % (name, e))
            return
        if event.find('detail/takv') is not None:
            self.contacts.append((name, event))      # a client announcing itself, not a detection
        else:
            self.events.append((name, event))
        if event.get('type') == 't-x-c-t':
            try:
                conn.sendall(_pong_event())
            except OSError:
                pass
            return
        # like TAK Server: pass the event on to the other connected clients
        with self._lock:
            others = [c for c in self._clients if c is not conn]
        for other in others:
            try:
                other.sendall(raw_event)
            except OSError:
                pass
        if self.on_event:
            self.on_event(name, event)

    def _chatter(self, conn):
        while not self._stopped:
            time.sleep(self.chatter_interval_s)
            try:
                conn.sendall(_other_user_event())
            except (OSError, ValueError):
                return

    def disconnect_clients(self):
        """Drop every client connection (to test reconnecting)."""
        with self._lock:
            clients, self._clients = self._clients, []
        for c in clients:
            try:
                c.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            c.close()

    def stop(self):
        self._stopped = True
        self._listener.close()
        self.disconnect_clients()


def _print_event(name, event):
    point = event.find('point')
    contact = event.find('detail/contact')
    remarks = event.find('detail/remarks')
    print("%s  %-14s %-34s %s %s  %s" % (
        name, event.get('type'), event.get('uid'),
        point.get('lat') if point is not None else '', point.get('lon') if point is not None else '',
        contact.get('callsign') if contact is not None else ''))
    if remarks is not None and remarks.text:
        print("    " + remarks.text)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--port', type=int, default=8089)
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--tcp', action='store_true', help='plain TCP instead of TLS')
    p.add_argument('--certs', default='./test_certs', help='folder from make_test_certs.sh')
    args = p.parse_args()
    server = MockTakServer(args.host, args.port,
                           certfile=args.certs + '/server.pem', keyfile=args.certs + '/server.key',
                           cafile=args.certs + '/ca.pem', use_tls=not args.tcp, on_event=_print_event)
    server.start()
    print("mock TAK server on %s://%s:%d" % ('tcp' if args.tcp else 'tls', args.host, server.port))
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()


if __name__ == '__main__':
    main()

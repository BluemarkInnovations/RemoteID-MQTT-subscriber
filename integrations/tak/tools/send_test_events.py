#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Send synthetic test objects to a TAK Server through the tak_gateway code.

Only made-up data: a test drone with its pilot flying a circle, a test aircraft
and a test sensor. Every callsign contains DRONESCOUT-TEST and the remarks say
TEST DATA, so on a shared or public server nobody takes them for real
detections. Nothing from MQTT or a real sensor is used.

    python3 integrations/tak/tools/send_test_events.py tcp://204.48.30.216:8087 --duration 60 --verify
    python3 integrations/tak/tools/send_test_events.py tls://localhost:8089 --certs test_certs --verify
    python3 integrations/tak/tools/send_test_events.py tcp://host:8087 --dry-run      # print the CoT only

--verify opens a second connection as another TAK user and counts the test
events the server passes on to it: the same check as watching the map in ATAK.
"""

import argparse
import os
import socket
import ssl
import sys
import threading
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..'))

from modules import test_objects                 # noqa: E402
from modules.test_objects import TEST, TEST_AIRCRAFT_ADDRESS   # noqa: E402
from integrations.tak import cot, tak_export     # noqa: E402

# uids of the test objects, to recognize them when they come back from the server
TEST_UIDS = {
    'dronescout-rid-' + TEST + '-DRONE',
    'dronescout-op-' + TEST + '-DRONE',
    'dronescout-adsb-' + TEST_AIRCRAFT_ADDRESS,
    'dronescout-' + TEST + '-SENSOR',
}


class Observer:
    """Second TAK user on the same server: counts the test events it receives."""

    def __init__(self, url, ssl_context):
        u = urlparse(url)
        raw = socket.create_connection((u.hostname, u.port), timeout=10)
        self.sock = ssl_context.wrap_socket(raw, server_hostname=u.hostname) if ssl_context else raw
        self.sock.settimeout(1.0)
        self.received = {}
        self.other = 0
        self._stop = False
        # announce ourselves like a TAK client does; some servers only forward to known clients
        now = time.time()
        presence = ('<event version="2.0" uid="%s-OBSERVER" type="a-f-G-U-C" how="h-e" time="%s" start="%s" '
                    'stale="%s"><point lat="0" lon="0" hae="9999999" ce="9999999" le="9999999"/><detail>'
                    '<contact callsign="%s-OBSERVER"/><remarks>TEST DATA</remarks></detail></event>'
                    % (TEST, cot.cot_time(now), cot.cot_time(now), cot.cot_time(now + 120), TEST))
        self.sock.sendall(presence.encode())
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        buf = b''
        while not self._stop:
            try:
                data = self.sock.recv(65536)
            except (socket.timeout, ssl.SSLWantReadError):
                continue
            except OSError:
                return
            if not data:
                return
            buf += data
            while b'</event>' in buf:
                end = buf.index(b'</event>') + len(b'</event>')
                start = buf.find(b'<event')
                chunk, buf = buf[start:end], buf[end:]
                try:
                    uid = ET.fromstring(chunk).get('uid', '')
                except ET.ParseError:
                    continue
                if uid in TEST_UIDS:
                    self.received[uid] = self.received.get(uid, 0) + 1
                else:
                    self.other += 1

    def close(self):
        self._stop = True
        try:
            self.sock.close()
        except OSError:
            pass


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('url', help='tls://host:port, tcp://host:port or udp://host:port')
    p.add_argument('--duration', type=float, default=60, help='seconds (default 60)')
    p.add_argument('--interval', type=float, default=1.0, help='seconds between updates (default 1)')
    p.add_argument('--center', type=float, nargs=2, default=(52.2200, 6.8900), metavar=('LAT', 'LON'))
    p.add_argument('--certs', help='folder with dronescout.pem/.key and ca.pem (integrations/tak/tools/make_test_certs.sh)')
    p.add_argument('--cert'); p.add_argument('--key'); p.add_argument('--ca')
    p.add_argument('--no-hostname-check', action='store_true')
    p.add_argument('--verify', action='store_true', help='check with a second connection that the server forwards')
    p.add_argument('--dry-run', action='store_true', help='print the CoT, send nothing')
    args = p.parse_args()

    cert, key, ca = args.cert, args.key, args.ca
    if args.certs:
        cert = cert or os.path.join(args.certs, 'dronescout.pem')
        key = key or os.path.join(args.certs, 'dronescout.key')
        ca = ca or os.path.join(args.certs, 'ca.pem')
    settings = {'sensor_stale_s': 60.0}          # test markers disappear soon after the test

    if args.dry_run:
        class PrintClient:
            def send(self, data):
                print(data.decode()); return True
            def close(self):
                pass
        client = PrintClient()
    else:
        client = tak_export.TakClient(args.url, certfile=cert, keyfile=key, cafile=ca,
                                      check_hostname=not args.no_hostname_check, ping_uid=TEST + '-GATEWAY',
                                      presence_callsign=TEST + '-GATEWAY')
    exporter = tak_export.TAKExporter(client, settings, min_interval_s=0)

    observer = None
    if args.verify and not args.dry_run:
        ctx = client._ssl_context            # same certificate and CA as the sender
        observer = Observer(args.url, ctx)
        time.sleep(1.0)

    start = time.time()
    end = start + (0 if args.dry_run else args.duration)
    sent_sensor = 0
    while True:
        now = time.time()
        exporter.on_remote_id(test_objects.drone(now, args.center))
        exporter.on_aircraft(test_objects.aircraft(now, args.center))
        if now - sent_sensor > 30:
            location, status = test_objects.sensor(now, args.center)
            exporter.on_location(location)
            exporter.on_status(status)
            sent_sensor = now
        if now >= end:
            break
        time.sleep(args.interval)

    if args.dry_run:
        return
    time.sleep(2.0)
    print("sent %d events, %d dropped, %d connection(s)" % (client.sent, client.dropped, client.connects))
    if observer:
        observer.close()
        total = sum(observer.received.values())
        print("second TAK user received %d of our events (%d other events from the server):"
              % (total, observer.other))
        for uid, n in sorted(observer.received.items()):
            print("    %-40s %d" % (uid, n))
        if total == 0:
            print("the server did not forward our events to the other user")
            exporter.close()
            sys.exit(1)
    exporter.close()


if __name__ == '__main__':
    main()

#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Send synthetic test objects to a SAPIENT fusion node through the sapient_gateway code.

Only made-up data (modules/test_objects.py): a test drone flying a circle, a test
aircraft and a test sensor, all with DRONESCOUT-TEST in their IDs. The test sensor
registers as its own SAPIENT node. Nothing from MQTT or a real sensor is used.

    python3 integrations/sapient/tools/send_test_events.py tcp://127.0.0.1:5020 --duration 30
    python3 integrations/sapient/tools/send_test_events.py tcp://127.0.0.1:5020 --verify-peer 127.0.0.1:5001

--verify-peer connects to the middleware as a DMM / fusion node (Apex: a "Peer"
connection) and counts the messages of the test node that the middleware passes on.
Apex only passes on messages that are valid BSI Flex 335 v2.0, so this checks both
the connection and the message contents.
"""

import argparse
import os
import socket
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from modules import test_objects                               # noqa: E402
from integrations.sapient import sapient_export, sapient_map as smap   # noqa: E402


class PeerObserver:
    """DMM-side connection that counts what the middleware forwards from one node."""

    def __init__(self, host, port, node_id):
        self.node_id = node_id
        self.sock = socket.create_connection((host, port), timeout=10)
        self.sock.settimeout(1.0)
        self.kinds = {}
        self.other = 0
        self._stop = False
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        buf = b''
        while not self._stop:
            try:
                data = self.sock.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                return
            if not data:
                return
            buf += data
            while len(buf) >= 4:
                length = int.from_bytes(buf[:4], 'little')
                if len(buf) < 4 + length:
                    break
                raw, buf = buf[4:4 + length], buf[4 + length:]
                try:
                    m = smap.parse(raw)
                except Exception:
                    continue
                if m.node_id == self.node_id:
                    kind = m.WhichOneof('content')
                    self.kinds[kind] = self.kinds.get(kind, 0) + 1
                else:
                    self.other += 1

    def close(self):
        self._stop = True
        self.sock.close()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('url', help='tcp://host:port (or tls://) of the fusion node / middleware')
    p.add_argument('--duration', type=float, default=30, help='seconds (default 30)')
    p.add_argument('--interval', type=float, default=1.0, help='seconds between updates (default 1)')
    p.add_argument('--center', type=float, nargs=2, default=(52.2200, 6.8900), metavar=('LAT', 'LON'))
    p.add_argument('--verify-peer', metavar='HOST:PORT', help='count what the middleware forwards to a DMM')
    args = p.parse_args()

    node_id = smap.node_uuid(test_objects.TEST_SENSOR)
    observer = None
    if args.verify_peer:
        host, port = args.verify_peer.rsplit(':', 1)
        observer = PeerObserver(host, int(port), node_id)
        time.sleep(0.5)

    # short stale time: the lost report of the aircraft can be seen at the end of the test
    exporter = sapient_export.SAPIENTExporter(args.url, stale_s=3.0, min_interval_s=0, heartbeat_s=5)
    print("test node %s (sensor %s)" % (node_id, test_objects.TEST_SENSOR))
    start = time.time()
    location, status = test_objects.sensor(start, args.center)
    exporter.on_location(location)
    exporter.on_status(status)
    while time.time() - start < args.duration:
        now = time.time()
        exporter.on_remote_id(test_objects.drone(now, args.center))
        exporter.on_aircraft(test_objects.aircraft(now, args.center))
        time.sleep(args.interval)
    time.sleep(5.0)          # tracks go "lost", last heartbeat
    exporter.close()
    time.sleep(1.0)
    print("sent %d messages, %d detections dropped, %d tasks rejected"
          % (exporter.sent, exporter.dropped, exporter.tasks_rejected))
    if observer:
        observer.close()
        print("the middleware forwarded from the test node: %s (%d messages of other nodes)"
              % (observer.kinds or 'nothing', observer.other))
        if not observer.kinds.get('detection_report'):
            print("no detection reports were forwarded: check the middleware log for validation errors")
            sys.exit(1)


if __name__ == '__main__':
    main()

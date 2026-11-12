#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Send synthetic test objects as ASTERIX through the asterix_gateway code.

Only made-up data (modules/test_objects.py): a test drone flying a circle
(CAT129 / CAT062), a test aircraft (CAT021) and a test sensor (CAT023), all with
DRONESCOUT-TEST in their IDs. Nothing from MQTT or a real sensor is used.

    python3 integrations/asterix/tools/send_test_events.py 192.168.1.20:8600 --sac 0 --sic 1 --duration 30
    python3 integrations/asterix/tools/send_test_events.py 127.0.0.1:8600 --all-categories --verify

--verify (local destinations only) listens on the destination port itself and
decodes everything that was sent.
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
from integrations.asterix import asterix_export, asterix_map as am   # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('destination', nargs='+', help='host:port (unicast or multicast)')
    p.add_argument('--sac', type=int, default=0)
    p.add_argument('--sic', type=int, default=1, help='SIC of the test sensor')
    p.add_argument('--gateway-sic', type=int, default=100)
    p.add_argument('--all-categories', action='store_true', help='also CAT062 / CAT065')
    p.add_argument('--duration', type=float, default=20)
    p.add_argument('--center', type=float, nargs=2, default=(52.2200, 6.8900), metavar=('LAT', 'LON'))
    p.add_argument('--verify', action='store_true', help='receive and decode on the (local) destination port')
    args = p.parse_args()

    dests = asterix_export.parse_destinations(args.destination)
    received, errors = {}, []
    if args.verify:
        rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        rx.bind(('0.0.0.0', dests[0][1]))
        rx.settimeout(0.5)

        def listen():
            while True:
                try:
                    data, _ = rx.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    return
                try:
                    for cat, recs in am.parse_datagram(data):
                        received[cat] = received.get(cat, 0) + len(recs)
                except Exception as e:
                    errors.append(e)
        threading.Thread(target=listen, daemon=True).start()

    categories = (21, 23, 129, 62, 65) if args.all_categories else (21, 23, 129)
    exp = asterix_export.AsterixExporter(dests, args.sac, {test_objects.TEST_SENSOR: args.sic},
                                         categories=categories, gateway_sic=args.gateway_sic,
                                         heartbeat_s=5, stale_s=3)
    start = time.time()
    _, status = test_objects.sensor(start, args.center)
    exp.on_status(status)
    while time.time() - start < args.duration:
        now = time.time()
        exp.on_remote_id(test_objects.drone(now, args.center))
        exp.on_aircraft(test_objects.aircraft(now, args.center))
        time.sleep(1.0)
    time.sleep(4.5)                                 # CAT062 end-of-track reports
    exp.close()
    time.sleep(0.5)
    print("sent %d datagrams, records %s, %d errors"
          % (exp.sent_datagrams, {c: n for c, n in exp.sent_records.items() if n}, exp.errors))
    if args.verify:
        print("received and decoded: %s, %d undecodable datagrams" % (received, len(errors)))
        if errors or not received:
            sys.exit(1)


if __name__ == '__main__':
    main()

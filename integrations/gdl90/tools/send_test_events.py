#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Send synthetic test traffic (no MQTT, no sensor needed) as GDL90 over UDP.

A test drone flying a circle (callsign DRONRONE: "DRON" + the last 4 characters of
DRONESCOUT-TEST-DRONE, address FF5561) and a test aircraft (address FFFFF1, callsign
DRONESCO), once per second.

    python3 integrations/gdl90/tools/send_test_events.py 192.168.10.255:4000 --duration 60
    python3 integrations/gdl90/tools/send_test_events.py 127.0.0.1:4000 --ownship
"""

import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from modules import test_objects                               # noqa: E402
from integrations.gdl90 import gdl90_export                    # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('destination', nargs='+', help='host:port')
    p.add_argument('--center', default='52.2185,6.9749', help='lat,lon of the test objects')
    p.add_argument('--duration', type=float, default=30)
    p.add_argument('--ownship', action='store_true', help='also send an ownship report south-west of the center')
    a = p.parse_args()

    center = tuple(float(x) for x in a.center.split(','))
    location, _ = test_objects.sensor(time.time(), center)
    exp = gdl90_export.GDL90Exporter(
        gdl90_export.parse_destinations(a.destination), start=False,
        ownship=(location['latitude'], location['longitude'], location['altitude MSL']) if a.ownship else None)
    end = time.time() + a.duration
    while time.time() < end:
        t = time.time()
        exp.on_remote_id(test_objects.drone(t, center))
        exp.on_aircraft(test_objects.aircraft(t, center))
        exp.flush()
        print('sent %d datagrams, %d errors' % (exp.sent, exp.errors))
        time.sleep(1)
    exp.close()
    sys.exit(0 if exp.sent and not exp.errors else 1)


if __name__ == '__main__':
    main()

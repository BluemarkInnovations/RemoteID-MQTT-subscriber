#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Send synthetic test events (no MQTT, no sensor needed) as UDP JSON datagrams.

A test drone flying a circle, a test aircraft and a test sensor, all named
DRONESCOUT-TEST, once per second.

    python3 integrations/udp/tools/send_test_events.py 239.255.86.1:8700
    python3 integrations/udp/tools/send_test_events.py 192.168.1.255:8700 --duration 30
"""

import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from modules import test_objects                               # noqa: E402
from integrations.udp import udp_export                        # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('destination', nargs='+', help='host:port (multicast, broadcast or unicast)')
    p.add_argument('--center', default='52.2185,6.9749', help='lat,lon of the test objects')
    p.add_argument('--duration', type=float, default=20)
    p.add_argument('--ttl', type=int, default=1, help='multicast TTL')
    p.add_argument('--interface', help='multicast: address of the interface to send from')
    a = p.parse_args()

    center = tuple(float(x) for x in a.center.split(','))
    exp = udp_export.UDPExporter(udp_export.parse_destinations(a.destination), source=test_objects.TEST + '-GATEWAY',
                                 min_interval_s=0, multicast_ttl=a.ttl, multicast_interface=a.interface)
    end = time.time() + a.duration
    while time.time() < end:
        t = time.time()
        location, status = test_objects.sensor(t, center)
        exp.on_location(location)
        exp.on_status(status)
        exp.on_remote_id(test_objects.drone(t, center))
        exp.on_aircraft(test_objects.aircraft(t, center))
        print('sent %d datagrams, %d errors' % (exp.sent, exp.errors))
        time.sleep(1)
    exp.close()
    sys.exit(0 if exp.sent and not exp.errors else 1)


if __name__ == '__main__':
    main()

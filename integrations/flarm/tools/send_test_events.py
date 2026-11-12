#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Serve synthetic FLARM traffic (no MQTT, no sensor needed): a test drone flying a
circle and a test aircraft, around the test sensor position, once per second.

    python3 integrations/flarm/tools/send_test_events.py --tcp-port 2000 --duration 120   # XCSoar: TCP client to this computer, port 2000
    python3 integrations/flarm/tools/send_test_events.py --udp 127.0.0.1:10110 --no-tcp
"""

import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from modules import test_objects                               # noqa: E402
from integrations.flarm import flarm_export                    # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--tcp-port', type=int, default=2000)
    p.add_argument('--tcp-bind', default='127.0.0.1')
    p.add_argument('--no-tcp', action='store_true')
    p.add_argument('--udp', nargs='*', default=[], metavar='HOST:PORT')
    p.add_argument('--center', default='52.2185,6.9749', help='lat,lon of the test objects')
    p.add_argument('--duration', type=float, default=30)
    a = p.parse_args()

    center = tuple(float(x) for x in a.center.split(','))
    location, _ = test_objects.sensor(time.time(), center)
    exp = flarm_export.FLARMExporter(
        tcp_port=None if a.no_tcp else a.tcp_port, tcp_bind=a.tcp_bind,
        udp_destinations=flarm_export.parse_destinations(a.udp), start=False,
        position=(location['latitude'], location['longitude'], location['altitude MSL']))
    if exp.server is not None:
        import threading
        threading.Thread(target=exp._accept_loop, daemon=True).start()
        print('TCP server on %s:%d' % (a.tcp_bind, exp.tcp_port))
    end = time.time() + a.duration
    while time.time() < end:
        t = time.time()
        exp.on_remote_id(test_objects.drone(t, center))
        exp.on_aircraft(test_objects.aircraft(t, center))
        n = exp.flush()
        print('sent %d sentences to %d TCP clients and %d UDP destinations' % (n, exp.clients, len(exp.udp_destinations)))
        time.sleep(1)
    exp.close()


if __name__ == '__main__':
    main()

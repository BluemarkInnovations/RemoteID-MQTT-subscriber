#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Send synthetic test events (no MQTT, no sensor needed) to a REST endpoint.

A test drone flying a circle, a test aircraft and a test sensor, all named
DRONESCOUT-TEST, once per second.

    python3 integrations/rest/tools/send_test_events.py http://127.0.0.1:8080/dronescout/events --token change-me
    python3 integrations/rest/tools/send_test_events.py URL --basic dronescout:change-me --duration 30
    python3 integrations/rest/tools/send_test_events.py URL --header X-API-Key:change-me
"""

import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from modules import test_objects                               # noqa: E402
from integrations.rest import rest_export                      # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('url')
    g = p.add_mutually_exclusive_group()
    g.add_argument('--token', help='bearer token')
    g.add_argument('--basic', metavar='USER:PASSWORD')
    g.add_argument('--header', metavar='NAME:VALUE', help='API-key header')
    p.add_argument('--center', default='52.2185,6.9749', help='lat,lon of the test objects')
    p.add_argument('--duration', type=float, default=20)
    p.add_argument('--insecure', action='store_true', help="https: don't verify the server certificate")
    p.add_argument('--cafile')
    a = p.parse_args()

    if a.token:
        headers = rest_export.auth_headers('bearer', token=a.token)
    elif a.basic:
        user, _, password = a.basic.partition(':')
        headers = rest_export.auth_headers('basic', username=user, password=password)
    elif a.header:
        name, _, value = a.header.partition(':')
        headers = rest_export.auth_headers('header', header_name=name, header_value=value)
    else:
        headers = {}
    center = tuple(float(x) for x in a.center.split(','))
    exp = rest_export.RESTExporter(a.url, headers=headers, source=test_objects.TEST + '-GATEWAY',
                                   min_interval_s=0, cafile=a.cafile, verify_tls=not a.insecure, start=False)
    end = time.time() + a.duration
    while time.time() < end:
        t = time.time()
        location, status = test_objects.sensor(t, center)
        exp.on_location(location)
        exp.on_status(status)
        exp.on_remote_id(test_objects.drone(t, center))
        exp.on_aircraft(test_objects.aircraft(t, center))
        exp.flush()
        print('posted %d events in %d requests, %d failed (HTTP %s)'
              % (exp.posted_events, exp.requests, exp.failed_requests, exp.last_status))
        time.sleep(1)
    exp.close()
    sys.exit(0 if exp.posted_events and not exp.dropped else 1)


if __name__ == '__main__':
    main()

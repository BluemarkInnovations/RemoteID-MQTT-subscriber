#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Run a CS-RID Finder with synthetic data (no MQTT, no sensor needed): registers at the SDSP and
forwards the B-RID of a test drone flying a circle, once per second.

    python3 integrations/csrid/tools/send_test_events.py --duration 10 --verify http://127.0.0.1:8088

The defaults match the Docker test SDSP (docker/compose.yaml, docker/finders.json).
"""

import argparse
import json
import os
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from modules import test_objects                               # noqa: E402
from integrations.csrid import csrid_export                    # noqa: E402

# the key of DRONESCOUT-TEST-FINDER in docker/finders.json: for the test SDSP only
TEST_KEY = 'e3153f3d52dc0c1f3eba4a4ab126bd372eb5666ab25023d769b0da05574b405e'


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--sdsp', default='coap://127.0.0.1:5683/csrid')
    p.add_argument('--finder-id', default=test_objects.TEST + '-FINDER')
    p.add_argument('--key', default=TEST_KEY, help='hex; "none" sends unauthenticated messages')
    p.add_argument('--center', default='52.2185,6.9749', help='lat,lon of the test drone')
    p.add_argument('--duration', type=float, default=10)
    p.add_argument('--verify', metavar='URL', help='JSON view of the reference SDSP: check it got the test drone')
    a = p.parse_args()

    center = tuple(float(x) for x in a.center.split(','))
    location, _ = test_objects.sensor(time.time(), center)
    finder = csrid_export.CSRIDFinder(
        a.sdsp, a.finder_id, key=None if a.key.lower() == 'none' else a.key,
        position=(location['latitude'], location['longitude'], location['altitude MSL']))
    finder.on_location(location)
    end = time.time() + 10
    while not finder.registered and time.time() < end:
        time.sleep(0.1)
    if not finder.registered:
        finder.close()
        sys.exit("not registered at %s: %s" % (a.sdsp, finder.stats()))
    end = time.time() + a.duration
    while time.time() < end:
        finder.on_remote_id(test_objects.drone(time.time(), center))
        time.sleep(1)
    time.sleep(0.5)
    print(finder.stats())
    finder.close()

    if a.verify:
        with urllib.request.urlopen(a.verify.rstrip('/') + '/uas', timeout=5) as r:
            uas = json.load(r)
        ua = uas.get('02:00:00:00:00:01')
        if not ua or a.finder_id not in ua.get('finders', {}):
            sys.exit("the SDSP has no B-RID of the test drone from %s" % a.finder_id)
        loc = ua.get('location', {})
        print("SDSP has the test drone: %s, %d messages, at %s, %s, height %s m"
              % (', '.join(b['uas_id'] for b in ua.get('basic_id', {}).values()), ua['messages'],
                 loc.get('latitude'), loc.get('longitude'), loc.get('height')))


if __name__ == '__main__':
    main()

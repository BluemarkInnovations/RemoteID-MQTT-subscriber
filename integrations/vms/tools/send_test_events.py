#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Send a synthetic drone alarm sequence (no MQTT, no sensor needed) to a VMS:
DRONESCOUT-TEST-DRONE detected, still present (every 5 s), lost; and the test sensor
degraded and normal again. With --verify-nx the Nx Witness event log is checked.

    python3 integrations/vms/tools/send_test_events.py --milestone 192.168.1.50:1234
    python3 integrations/vms/tools/send_test_events.py --nx https://127.0.0.1:7001 --user admin --password ... --insecure --verify-nx
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
from integrations.vms import vms_alarms, vms_export            # noqa: E402


def nx_log_captions(nx, since_ms):
    req = urllib.request.Request(nx.url + '/rest/v4/events/log', headers={'Authorization': 'Bearer ' + nx._token})
    with urllib.request.urlopen(req, timeout=10, context=nx._context) as resp:
        log = json.loads(resp.read())
    return [(e['eventData'].get('caption'), e['eventData'].get('state')) for e in log
            if e['eventData'].get('type') == 'generic' and e['eventData'].get('source') == nx.source
            and e['timestampMs'] >= since_ms]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--milestone', metavar='HOST:PORT')
    p.add_argument('--milestone-udp', action='store_true')
    p.add_argument('--nx', metavar='URL')
    p.add_argument('--user', default='admin')
    p.add_argument('--password')
    p.add_argument('--insecure', action='store_true', help="Nx: don't verify the server certificate")
    p.add_argument('--verify-nx', action='store_true', help='check the Nx event log afterwards')
    p.add_argument('--present', type=float, default=12, help='seconds the test drone is present')
    p.add_argument('--center', default='52.2185,6.9749')
    a = p.parse_args()

    backends = []
    if a.milestone:
        host, port = a.milestone.rsplit(':', 1)
        backends.append(vms_export.MilestoneGenericEvents(host, int(port), 'udp' if a.milestone_udp else 'tcp'))
    nx = None
    if a.nx:
        nx = vms_export.NxWitnessGenericEvents(a.nx, a.user, a.password, source='DroneScout-TEST', verify_tls=not a.insecure)
        backends.append(nx)
    if not backends:
        p.error('give --milestone and/or --nx')
    start_ms = int(time.time() * 1000) - 1000
    center = tuple(float(x) for x in a.center.split(','))
    exp = vms_export.VMSExporter(backends, vms_alarms.Alarms(lost_s=3, update_s=5), start=False)
    _, status = test_objects.sensor(time.time(), center)
    exp.on_status(dict(status, status='degraded', **{'status details': 'TEST DATA: WLAN radio wlan1 stalled'}))
    end = time.time() + a.present
    while time.time() < end:
        exp.on_remote_id(test_objects.drone(time.time(), center))
        exp.tick()
        exp.drain()
        time.sleep(1)
    print('drone gone, waiting for the lost event')
    for _ in range(5):
        time.sleep(1)
        exp.tick()
    exp.on_status(status)
    exp.close()
    print('sent %s, failed %s' % (exp.sent, exp.failed))
    ok = not any(exp.failed.values())
    if nx and a.verify_nx:
        time.sleep(1)
        captions = nx_log_captions(nx, start_ms)
        print('Nx event log (generic events from DroneScout-TEST):')
        for c in captions:
            print('   ', c)
        ok = ok and any('detected' in (c or '') for c, _ in captions)
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()

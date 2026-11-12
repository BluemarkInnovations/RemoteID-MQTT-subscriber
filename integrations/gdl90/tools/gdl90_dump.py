#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Receive GDL90 over UDP and print every message (decoded with gdl90_map).

    python3 integrations/gdl90/tools/gdl90_dump.py [--port 4000]

The Docker receiver (integrations/gdl90/docker) decodes with an independent
library instead; use that one to check the encoder.
"""

import argparse
import os
import signal
import socket
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from integrations.gdl90 import gdl90_map as gm                 # noqa: E402


def describe(msg):
    if msg[0] == gm.MSG_HEARTBEAT:
        ts = msg[3] | (msg[4] << 8) | ((msg[2] >> 7) << 16)
        return 'heartbeat  %02d:%02d:%02d UTC, GPS %s' % (ts // 3600, ts // 60 % 60, ts % 60,
                                                          'valid' if msg[1] & 0x80 else 'invalid')
    if msg[0] in (gm.MSG_OWNSHIP, gm.MSG_TRAFFIC):
        r = gm.parse_report(msg)
        return ('%-10s %06X type %d %-8s %.5f %.5f %s ft %s kt %s fpm trk %.0f emitter %d NIC %d NACp %d %s'
                % ('ownship' if msg[0] == gm.MSG_OWNSHIP else 'traffic', r['address'], r['address_type'],
                   r['callsign'], r['lat'], r['lon'], r['alt_ft'], r['speed_kt'], r['vs_fpm'], r['track'],
                   r['emitter'], r['nic'], r['nacp'], 'airborne' if r['misc'] & 8 else 'on ground'))
    if msg[0] == gm.MSG_OWNSHIP_GEO_ALT:
        return 'geo alt    %d ft' % (int.from_bytes(msg[1:3], 'big', signed=True) * 5)
    if msg[0] == gm.MSG_FOREFLIGHT and msg[1] == 0:
        return 'device     %s / %s' % (msg[11:19].decode('ascii').strip(), msg[19:35].decode('ascii').strip())
    return 'message %d (%d bytes)' % (msg[0], len(msg))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--port', type=int, default=4000)
    a = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', a.port))
    print('listening on UDP port %d' % a.port)
    try:
        while True:
            data, src = s.recvfrom(65535)
            messages = gm.unframe(data)
            if not messages:
                print('no valid GDL90 frame from %s (%d bytes)' % (src[0], len(data)))
            for m in messages:
                print(describe(m))
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()

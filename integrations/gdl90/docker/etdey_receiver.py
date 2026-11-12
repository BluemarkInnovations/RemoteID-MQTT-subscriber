#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""GDL90 test receiver: decodes with the independent etdey/gdl90 library (MIT,
https://github.com/etdey/gdl90), not with our own code, so it checks the encoder.

Prints every message, counts bad frames (CRC) and unknown messages, and a summary
every 10 s. Runs in the Docker container (compose.yaml); outside it:
    PYTHONPATH=/path/to/etdey/gdl90 python3 etdey_receiver.py --port 4000
"""

import argparse
import signal
import socket
import sys
import time

from gdl90 import decoder as etdey_decoder
from gdl90 import messages as etdey_messages
from gdl90.fcs import crcCheck


def frames(data):
    for chunk in data.split(b'\x7e'):
        if chunk:
            yield chunk


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--port', type=int, default=4000)
    a = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    unescape = etdey_decoder.Decoder()._unescape
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', a.port))
    print('etdey/gdl90 receiver listening on UDP port %d' % a.port)
    counts, bad_crc, unknown, last = {}, 0, 0, time.monotonic()
    try:
        while True:
            data, src = s.recvfrom(65535)
            for escaped in frames(data):
                raw = unescape(bytearray(escaped))
                msg, crc = raw[:-2], raw[-2:]
                if len(raw) < 3 or not crcCheck(msg, crc):
                    bad_crc += 1
                    print('BAD CRC from %s: %s' % (src[0], bytes(escaped).hex()))
                    continue
                m = etdey_messages.messageToObject(msg)
                if not m:
                    unknown += 1
                    print('message %d not decoded by etdey/gdl90 (%d bytes)' % (msg[0], len(msg)))
                    continue
                counts[m.MsgType] = counts.get(m.MsgType, 0) + 1
                if m.MsgType == 'TrafficReport' or m.MsgType == 'OwnshipReport':
                    print('%-13s %06X type %d %-8s %.5f %.5f %d ft %d kt %d fpm trk %.0f emitter %d NIC %d NACp %d misc %x'
                          % (m.MsgType, m.Address, m.Type, m.CallSign, m.Latitude, m.Longitude, m.Altitude,
                             m.HVelocity, m.VVelocity, m.TrackHeading, m.EmitterCat, m.NavIntegrityCat,
                             m.NavAccuracyCat, m.Misc))
                elif m.MsgType == 'Heartbeat':
                    print('Heartbeat     status %02x %02x timestamp %d s' % (m.StatusByte1, m.StatusByte2, m.TimeStamp))
                else:
                    print(m)
            if time.monotonic() - last > 10:
                last = time.monotonic()
                print('decoded: %s, bad CRC: %d, not decoded: %d' % (counts, bad_crc, unknown))
    except (KeyboardInterrupt, SystemExit):
        pass
    print('decoded: %s, bad CRC: %d, not decoded: %d' % (counts, bad_crc, unknown))


if __name__ == '__main__':
    main()

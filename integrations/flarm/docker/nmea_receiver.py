#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""FLARM NMEA test receiver: parses with the independent pynmea2 library (MIT), not with
our own code. pynmea2 checks every checksum and decodes $GPRMC / $GPGGA; the FLARM
sentences ($PFLAU, $PFLAA) it parses as proprietary sentences (manufacturer "FLA"),
whose fields are then checked here against the FLARM data port specification.

    python3 nmea_receiver.py --udp 10110                 # listen for UDP datagrams
    python3 nmea_receiver.py --tcp host.docker.internal:2000   # connect to a TCP server, like XCSoar
"""

import argparse
import re
import signal
import socket
import sys
import time

import pynmea2

HEX6 = re.compile(r'^[0-9A-F]{6}$')
INT = re.compile(r'^-?\d+$')


def check_pflaa(f):
    """Problems in the data fields of a $PFLAA (after the sentence name), [] if fine."""
    problems = []
    if len(f) < 11:
        return ['%d fields, at least 11 expected' % len(f)]
    if f[0] not in ('0', '1', '2', '3'):
        problems.append('AlarmLevel %r' % f[0])
    for name, v in (('RelativeNorth', f[1]), ('RelativeEast', f[2])):
        if not INT.match(v):
            problems.append('%s %r is not an integer' % (name, v))
    if f[3] and not INT.match(f[3]):
        problems.append('RelativeVertical %r is not an integer' % f[3])
    if f[4] not in ('0', '1', '2'):
        problems.append('IDType %r' % f[4])
    if not HEX6.match(f[5]):
        problems.append('ID %r is not 6 hex digits' % f[5])
    if f[6] and not (INT.match(f[6]) and 0 <= int(f[6]) <= 359):
        problems.append('Track %r' % f[6])
    if f[8] and not INT.match(f[8]):
        problems.append('GroundSpeed %r' % f[8])
    if f[9]:
        try:
            if abs(float(f[9])) > 32.7:
                problems.append('ClimbRate %r out of range' % f[9])
        except ValueError:
            problems.append('ClimbRate %r' % f[9])
    if not re.match(r'^[0-9A-F]$', f[10]):
        problems.append('AcftType %r' % f[10])
    return problems


class Stats:
    def __init__(self):
        self.counts, self.errors, self.last = {}, 0, time.monotonic()

    def line(self, text):
        text = text.strip()
        if not text:
            return
        try:
            msg = pynmea2.parse(text, check=True)
        except pynmea2.ParseError as e:
            self.errors += 1
            print('PARSE ERROR: %s (%s)' % (text, e))
            return
        if isinstance(msg, pynmea2.ProprietarySentence):
            name = 'P' + msg.manufacturer + (msg.data[0] if msg.data else '')
            fields = msg.data[1:]
            if name == 'PFLAA':
                problems = check_pflaa(fields)
                if problems:
                    self.errors += 1
                    print('BAD PFLAA %s: %s' % (text, '; '.join(problems)))
                    return
                print('PFLAA  id %s (type %s) acft %s  %s m N  %s m E  %s m up  trk %s  %s m/s  climb %s'
                      % (fields[5], fields[4], fields[10], fields[1], fields[2], fields[3] or '?', fields[6] or '?',
                         fields[8] or '?', fields[9] or '?'))
            elif name == 'PFLAU':
                print('PFLAU  %d targets, TX %s, GPS %s' % (int(fields[0] or 0), fields[1], fields[2]))
            else:
                print(text)
        else:
            name = msg.sentence_type
            if name == 'RMC':
                print('RMC    %.5f %.5f %s %s' % (msg.latitude, msg.longitude, msg.datestamp, msg.timestamp))
            elif name == 'GGA':
                print('GGA    %.5f %.5f %s m, %s satellites' % (msg.latitude, msg.longitude, msg.altitude, msg.num_sats))
            else:
                print(text)
        self.counts[name] = self.counts.get(name, 0) + 1
        if time.monotonic() - self.last > 10:
            self.last = time.monotonic()
            print('sentences: %s, errors: %d' % (self.counts, self.errors))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--udp', type=int, metavar='PORT')
    p.add_argument('--tcp', metavar='HOST:PORT')
    a = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    stats = Stats()
    try:
        if a.tcp:
            host, port = a.tcp.rsplit(':', 1)
            while True:
                try:
                    with socket.create_connection((host, int(port)), timeout=10) as s:
                        print('connected to %s' % a.tcp)
                        buf = b''
                        while True:
                            data = s.recv(4096)
                            if not data:
                                break
                            buf += data
                            *lines, buf = buf.split(b'\n')
                            for line in lines:
                                stats.line(line.decode('ascii', 'replace'))
                except OSError as e:
                    print('%s: %s, retrying in 5 s' % (a.tcp, e))
                time.sleep(5)
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', a.udp or 10110))
            print('pynmea2 receiver listening on UDP port %d' % (a.udp or 10110))
            while True:
                data, _ = s.recvfrom(65535)
                for line in data.decode('ascii', 'replace').split('\n'):
                    stats.line(line)
    except (KeyboardInterrupt, SystemExit):
        pass
    print('sentences: %s, errors: %d' % (stats.counts, stats.errors))


if __name__ == '__main__':
    main()

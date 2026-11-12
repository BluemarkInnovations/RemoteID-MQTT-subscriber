#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Receive ASTERIX over UDP, decode it and print a summary of every record.

Decodes CAT021/023/062/065 with libasterix and CAT129 with our own decoder (no
common decoder knows CAT129). With --pcap the datagrams are also written to a
pcap file to open in Wireshark (Decode As: UDP port -> ASTERIX).

    python3 integrations/asterix/tools/asterix_dump.py --port 8600
    python3 integrations/asterix/tools/asterix_dump.py --port 8600 --group 239.1.2.3 --pcap out.pcap
"""

import argparse
import os
import signal
import socket
import struct
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from integrations.asterix import asterix_map as am            # noqa: E402


def _q(rec, item, sub=None, unit=None):
    i = rec.get_item(item)
    if i is None:
        return None
    v = i.variation
    if sub:
        s = v.get_item(sub)
        if s is None:
            return None
        v = s.variation
    return v.content.as_quantity(unit) if unit else v.content.as_uint()


def describe(cat, rec):
    if cat == 129:
        return ('CAT129 SIC %s serial %r %.6f %.6f AMSL %s AGL %s v=(%s,%s,%s) %s'
                % (rec.get('SIC'), rec.get('serial'), rec.get('lat'), rec.get('lon'), rec.get('amsl'),
                   rec.get('agl'), rec.get('vx'), rec.get('vy'), rec.get('vv'), (rec.get('SP') or b'').decode()))
    sic = _q(rec, '010', 'SIC')
    if cat == 21:
        ident = rec.get_item('170')
        return ('CAT021 SIC %s address %06X %s %.6f %.6f FL %s track %s'
                % (sic, _q(rec, '080'), ident.variation.content.as_string().strip() if ident else '',
                   _q(rec, '131', 'LAT', '°'), _q(rec, '131', 'LON', '°'), _q(rec, '145', None, 'FL'),
                   _q(rec, '161', 'TRNUM')))
    if cat == 23:
        rtype = _q(rec, '000')
        if rtype == 2:
            stat = rec.get_item('110').variation.get_item('STAT').variation.content.as_uint()
            return 'CAT023 SIC %s service %s status %s' % (sic, _q(rec, '015', 'SID'), stat)
        return 'CAT023 SIC %s ground station status' % sic
    if cat == 62:
        return ('CAT062 SIC %s track %s %.6f %.6f'
                % (sic, _q(rec, '040'), _q(rec, '105', 'LAT', '°'), _q(rec, '105', 'LON', '°')))
    if cat == 65:
        return 'CAT065 SIC %s SDPS status' % sic
    return 'CAT%03d' % cat


class PcapWriter:
    def __init__(self, path):
        self.f = open(path, 'wb')
        self.f.write(struct.pack('<IHHiIII', 0xa1b2c3d4, 2, 4, 0, 0, 65535, 101))     # raw IP

    def write(self, data, src, dst_port):
        udp = struct.pack('!HHHH', src[1], dst_port, 8 + len(data), 0) + data
        ip = struct.pack('!BBHHHBBH4s4s', 0x45, 0, 20 + len(udp), 0, 0, 64, 17, 0,
                         socket.inet_aton(src[0]), socket.inet_aton('127.0.0.1'))
        t = time.time()
        self.f.write(struct.pack('<IIII', int(t), int((t % 1) * 1e6), len(ip + udp), len(ip + udp)) + ip + udp)
        self.f.flush()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--port', type=int, default=8600)
    p.add_argument('--bind', default='0.0.0.0')
    p.add_argument('--group', help='multicast group to join')
    p.add_argument('--pcap', help='also write the datagrams to this pcap file')
    p.add_argument('--quiet', action='store_true', help='only print counts every 10 s')
    args = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)          # also when redirected to a file / docker logs
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((args.bind, args.port))
    if args.group:
        s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                     socket.inet_aton(args.group) + socket.inet_aton('0.0.0.0'))
    pcap = PcapWriter(args.pcap) if args.pcap else None
    counts = {}
    print("listening on %s:%d" % (args.bind, args.port))
    try:
        _receive(s, args, pcap, counts)
    except (KeyboardInterrupt, SystemExit):
        pass
    print("records per category: %s" % counts)


def _receive(s, args, pcap, counts):
    errors, last = 0, time.monotonic()
    while True:
        data, src = s.recvfrom(65535)
        if pcap:
            pcap.write(data, src, args.port)
        try:
            for cat, recs in am.parse_datagram(data):
                counts[cat] = counts.get(cat, 0) + len(recs)
                if not args.quiet:
                    for rec in recs:
                        print(describe(cat, rec))
        except Exception as e:
            errors += 1
            print("can't decode a datagram from %s: %s" % (src[0], e))
        if time.monotonic() - last > 10:
            last = time.monotonic()
            print("records per category: %s, %d undecodable datagrams" % (counts, errors))


if __name__ == '__main__':
    main()

#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Example UDP receiver for udp_gateway.py: one JSON event per datagram.

Standard library only and no imports from this repository, so it can be copied as is.

    python3 integrations/udp/tools/udp_listen.py --group 239.255.86.1      # multicast (default port 8700)
    python3 integrations/udp/tools/udp_listen.py                           # broadcast / unicast to this host
    python3 integrations/udp/tools/udp_listen.py --group 239.255.86.1 --interface 192.168.1.10 --json

Several receivers on one computer can listen to the same port (multicast and
broadcast are delivered to all of them).
"""

import argparse
import json
import signal
import socket
import sys


def summary(e):
    pos = ' %.6f %.6f' % (e['latitude'], e['longitude']) if 'latitude' in e else ''
    t = e.get('type')
    if t == 'drone':
        return 'drone %s via %s%s height %s m' % (e.get('id'), e.get('sensor_id'), pos, e.get('height_m'))
    if t == 'aircraft':
        return 'aircraft %s %s via %s%s alt %s m' % (e.get('id'), e.get('callsign', ''), e.get('sensor_id'),
                                                     pos, e.get('altitude_m'))
    if t == 'sensor_status':
        return 'sensor %s status %s' % (e.get('sensor_id'), e.get('status'))
    if t == 'sensor_location':
        return 'sensor %s location%s' % (e.get('sensor_id'), pos)
    return '%s event' % t


def open_socket(port, group=None, interface='0.0.0.0', bind=''):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, 'SO_REUSEPORT'):
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    s.bind((bind, port))           # '' : also receives broadcast; a unicast address would not
    if group:
        s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(group) + socket.inet_aton(interface))
    return s


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--port', type=int, default=8700)
    p.add_argument('--group', help='multicast group to join, e.g. 239.255.86.1')
    p.add_argument('--interface', default='0.0.0.0', help='multicast: address of the interface to join on')
    p.add_argument('--json', action='store_true', help='print the JSON as received')
    a = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    s = open_socket(a.port, a.group, a.interface)
    print('listening on UDP port %d%s' % (a.port, ' for group %s' % a.group if a.group else ''))
    n = bad = 0
    try:
        while True:
            data, src = s.recvfrom(65535)
            try:
                event = json.loads(data)
            except ValueError:
                bad += 1
                print('not JSON from %s:%d (%d bytes)' % (src[0], src[1], len(data)))
                continue
            n += 1
            print(data.decode() if a.json else '%s: %s' % (event.get('source'), summary(event)))
    except (KeyboardInterrupt, SystemExit):
        pass
    print('%d events, %d datagrams not JSON' % (n, bad))


if __name__ == '__main__':
    main()

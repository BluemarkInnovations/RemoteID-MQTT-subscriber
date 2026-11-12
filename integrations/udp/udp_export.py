# (c) Bluemark Innovations BV
# MIT license
"""UDP exporter: every event as one JSON datagram, to multicast, broadcast and/or unicast
destinations on the local network.

    {"schema_version": 1, "source": "dronescout-gateway", "type": "drone", "id": "...", ...}

The events are the ones the REST output sends (modules/json_events.py), one per
datagram, so a receiver needs no framing: one recvfrom() is one complete JSON object.
UDP has no delivery guarantee and no authentication: for a trusted local network.
"""

import ipaddress
import json
import socket
import time

from modules import decode
from modules import exporters
from modules import json_events


def destination_kind(host):
    """'multicast', 'broadcast' (255.255.255.255 or an address ending in .255), 'unicast' or 'name'."""
    try:
        ip = ipaddress.IPv4Address(host)
    except ValueError:
        return 'name'
    if ip.is_multicast:
        return 'multicast'
    if host == '255.255.255.255' or host.endswith('.255'):
        return 'broadcast'          # subnet-directed broadcast for a /24; set it explicitly for other masks
    return 'unicast'


class UDPExporter(exporters.Exporter):
    name = "udp"

    def __init__(self, destinations, source='dronescout-gateway', min_interval_s=1.0, send_drones=True,
                 send_aircraft=True, send_status=True, multicast_ttl=1, multicast_interface=None,
                 max_datagram=1400, log=print, clock=time.monotonic):
        self.destinations = [(str(h), int(p)) for h, p in destinations]
        if not self.destinations:
            raise ValueError("no UDP destinations")
        self.source = source
        self.min_interval_s = min_interval_s
        self.send_drones = send_drones
        self.send_aircraft = send_aircraft
        self.send_status = send_status
        self.max_datagram = max_datagram
        self.log = log
        self._clock = clock
        self._last_sent = {}
        self.sent = 0
        self.errors = 0
        self.too_large = 0
        self.rate_limited = 0

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, multicast_ttl)
        if multicast_interface:
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(multicast_interface))
        for host, port in self.destinations:
            self.log("UDP: sending to %s:%d (%s)" % (host, port, destination_kind(host)))

    def _send(self, key, event):
        if key is not None:
            now = self._clock()
            last = self._last_sent.get(key)
            if last is not None and now - last < self.min_interval_s:
                self.rate_limited += 1
                return
            self._last_sent[key] = now
            if len(self._last_sent) > 20000:
                self._last_sent = {k: v for k, v in self._last_sent.items() if v >= now - 600}
        data = json.dumps(dict({'schema_version': json_events.SCHEMA_VERSION, 'source': self.source}, **event),
                          separators=(',', ':')).encode()
        if len(data) > self.max_datagram:
            # larger datagrams are fragmented by IP and often lost on the way
            self.too_large += 1
            if self.too_large in (1, 100) or self.too_large % 10000 == 0:
                self.log("UDP: %s event of %d bytes is larger than %d, not sent (%d so far)"
                         % (event.get('type'), len(data), self.max_datagram, self.too_large))
            return
        for dest in self.destinations:
            try:
                self.sock.sendto(data, dest)
                self.sent += 1
            except OSError as e:
                self.errors += 1
                if self.errors in (1, 10, 100) or self.errors % 1000 == 0:
                    self.log("UDP: sending to %s:%d failed: %s (%d errors)" % (dest[0], dest[1], e, self.errors))

    def on_remote_id(self, msg):
        if self.send_drones:
            self._send(('drone', msg.sensor_id, decode.drone_id(msg)), json_events.drone_event(msg))

    def on_aircraft(self, msg):
        if self.send_aircraft:
            self._send(('aircraft', msg.sensor_id, msg.get('type'), msg.get('address')),
                       json_events.aircraft_event(msg))

    def on_status(self, status_json):
        if self.send_status:
            self._send(None, json_events.status_event(status_json))

    def on_location(self, location_json):
        if self.send_status:
            self._send(None, json_events.location_event(location_json))

    def close(self):
        self.sock.close()


def parse_destinations(items):
    """'host:port' strings (or (host, port) tuples) -> [(host, port)]."""
    out = []
    for d in items:
        if isinstance(d, (tuple, list)):
            out.append((str(d[0]), int(d[1])))
        else:
            host, port = str(d).rsplit(':', 1)
            out.append((host, int(port)))
    return out

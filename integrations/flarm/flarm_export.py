# (c) Bluemark Innovations BV
# MIT license
"""FLARM NMEA exporter: drones and aircraft as FLARM traffic for XCSoar, LK8000, ...

Every second the own position ($GPRMC, $GPGGA), the status ($PFLAU) and one $PFLAA per
drone / aircraft that sent an update in the last stale_s seconds, within max_range_m.
Sent to every connected TCP client (the gateway is a TCP server, like a FLARM Wi-Fi
bridge on port 2000) and/or to UDP destinations.
"""

import math
import socket
import threading
import time

from modules import decode
from modules import exporters
from integrations.flarm import flarm_map as fm


class FLARMExporter(exporters.Exporter):
    name = "flarm"

    def __init__(self, tcp_port=2000, tcp_bind='0.0.0.0', udp_destinations=(), position=None,
                 position_sensor=None, height_agl_m=0.0, send_drones=True, send_aircraft=True,
                 max_range_m=50000.0, stale_s=5.0, interval_s=1.0, max_clients=10, log=print,
                 clock=time.monotonic, start=True):
        self.udp_destinations = [(str(h), int(p)) for h, p in udp_destinations]
        if tcp_port is None and not self.udp_destinations:
            raise ValueError("no TCP port and no UDP destinations")
        self.position = tuple(position) if position else None           # (lat, lon, altitude MSL m)
        self.position_sensor = position_sensor
        self.height_agl_m = height_agl_m
        self.send_drones = send_drones
        self.send_aircraft = send_aircraft
        self.max_range_m = max_range_m
        self.stale_s = stale_s
        self.interval_s = interval_s
        self.max_clients = max_clients
        self.log = log
        self._clock = clock
        self._lock = threading.Lock()
        self._targets = {}                 # key -> (time seen, kind, message)
        self._clients = []
        self.sent_lines = 0
        self.errors = 0
        self.out_of_range = 0
        self._warned_no_position = False
        self._closed = False
        self._stop = threading.Event()

        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.server = None
        if tcp_port is not None:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((tcp_bind, tcp_port))
            self.server.listen(5)
            self.tcp_port = self.server.getsockname()[1]
        self._threads = []
        if start:
            if self.server is not None:
                self._threads.append(threading.Thread(target=self._accept_loop, name='flarm-accept', daemon=True))
            self._threads.append(threading.Thread(target=self._loop, name='flarm', daemon=True))
            for t in self._threads:
                t.start()

    # --- exporter callbacks ---------------------------------------------------------------------

    def on_remote_id(self, msg):
        if self.send_drones and msg.uas.location.has_position:
            with self._lock:
                self._targets[('drone', decode.drone_id(msg))] = (self._clock(), 'drone', msg)

    def on_aircraft(self, msg):
        if self.send_aircraft:
            with self._lock:
                self._targets[('aircraft', msg.get('type'), msg.get('address'))] = (self._clock(), 'aircraft', msg)

    def on_location(self, location_json):
        if self.position_sensor is not None and str(location_json.get('sensor ID')) == str(self.position_sensor):
            lat, lon = location_json.get('latitude'), location_json.get('longitude')
            if lat is not None and lon is not None:
                self.position = (lat, lon, location_json.get('altitude MSL') or 0.0)

    # --- output ---------------------------------------------------------------------------------

    def lines(self, now_wall=None):
        """The sentences of one second."""
        now = self._clock()
        with self._lock:
            for key in [k for k, (seen, _, _) in self._targets.items() if now - seen > self.stale_s]:
                del self._targets[key]
            targets = list(self._targets.values())
        if self.position is None:
            if not self._warned_no_position:
                self._warned_no_position = True
                self.log("FLARM: no own position yet (flarm_position / flarm_position_sensor): no traffic sent")
            return [fm.pflau(0, False)]
        lat, lon, msl = self.position
        own = (lat, lon, msl, self.height_agl_m)
        out = []
        for _, kind, msg in targets:
            line = fm.drone_target(msg, own) if kind == 'drone' else fm.aircraft_target(msg, own)
            if line is None:
                continue
            fields = fm.parse_sentence(line)
            if math.hypot(int(fields[2]), int(fields[3])) > self.max_range_m:
                self.out_of_range += 1
                continue
            out.append(line)
        return [fm.gprmc(lat, lon, now_wall), fm.gpgga(lat, lon, msl, now_wall), fm.pflau(len(out), True)] + out

    def flush(self, now_wall=None):
        data = ''.join(self.lines(now_wall)).encode('ascii')
        n = data.count(b'\n')
        for dest in self.udp_destinations:
            try:
                self.udp.sendto(data, dest)
                self.sent_lines += n
            except OSError as e:
                self.errors += 1
                if self.errors in (1, 10, 100) or self.errors % 1000 == 0:
                    self.log("FLARM: sending to %s:%d failed: %s" % (dest[0], dest[1], e))
        with self._lock:
            clients = list(self._clients)
        for c in clients:
            try:
                c.sendall(data)
                self.sent_lines += n
            except OSError:
                self._drop(c)
        return n

    def _drop(self, client):
        with self._lock:
            if client in self._clients:
                self._clients.remove(client)
        try:
            peer = client.getpeername()
        except OSError:
            peer = ('?', 0)
        try:
            client.close()
        except OSError:
            pass
        self.log("FLARM: client %s:%d disconnected" % peer)

    def _accept_loop(self):
        while not self._closed:
            try:
                client, addr = self.server.accept()
            except OSError:
                return
            client.settimeout(2.0)          # a client that stops reading is dropped, not waited for
            with self._lock:
                if len(self._clients) >= self.max_clients:
                    client.close()
                    self.log("FLARM: refused %s:%d, already %d clients" % (addr[0], addr[1], self.max_clients))
                    continue
                self._clients.append(client)
            self.log("FLARM: client %s:%d connected" % addr)

    @property
    def clients(self):
        with self._lock:
            return len(self._clients)

    def _loop(self):
        while not self._closed:
            start = time.monotonic()
            try:
                self.flush()
            except Exception as e:                # never let the thread die
                self.errors += 1
                self.log("FLARM: error: %r" % e)
            self._stop.wait(max(0.0, self.interval_s - (time.monotonic() - start)))

    def close(self):
        self._closed = True
        self._stop.set()
        if self.server is not None:
            try:
                self.server.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.server.close()
        for t in self._threads:
            t.join(timeout=self.interval_s + 1)
        with self._lock:
            clients, self._clients = self._clients, []
        for c in clients:
            try:
                c.close()
            except OSError:
                pass
        self.udp.close()


def parse_destinations(items):
    out = []
    for d in items:
        if isinstance(d, (tuple, list)):
            out.append((str(d[0]), int(d[1])))
        else:
            host, port = str(d).rsplit(':', 1)
            out.append((host, int(port)))
    return out

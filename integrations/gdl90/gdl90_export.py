# (c) Bluemark Innovations BV
# MIT license
"""GDL90 exporter: drones and aircraft as traffic for cockpit apps (EFBs), over UDP.

Every second: a heartbeat, the ForeFlight ID message, optionally an ownship report
(a fixed position: ground displays only), and one traffic report per drone / aircraft
that sent an update since the previous second. Each GDL90 message is sent as its own
UDP datagram, like Stratux does, to every destination (normally broadcast to port 4000).
"""

import socket
import threading
import time

from modules import decode
from modules import exporters
from integrations.gdl90 import gdl90_map as gm


class GDL90Exporter(exporters.Exporter):
    name = "gdl90"

    def __init__(self, destinations, send_drones=True, send_aircraft=True, ownship=None, ownship_sensor=None,
                 device_name='DroneSct', device_long_name='DroneScout', drone_nic=None, interval_s=1.0,
                 log=print, start=True):
        self.destinations = [(str(h), int(p)) for h, p in destinations]
        if not self.destinations:
            raise ValueError("no GDL90 destinations")
        self.send_drones = send_drones
        self.send_aircraft = send_aircraft
        self.ownship = tuple(ownship) if ownship else None          # (lat, lon, altitude MSL m)
        self.ownship_sensor = ownship_sensor
        self.device_name = device_name
        self.device_long_name = device_long_name
        self.drone_nic = drone_nic
        self.interval_s = interval_s
        self.log = log
        self.sent = 0
        self.errors = 0
        self._lock = threading.Lock()
        self._pending = {}                                            # key -> traffic message bytes
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._closed = False
        self._stop = threading.Event()
        self._thread = None
        if start:
            self._thread = threading.Thread(target=self._loop, name='gdl90', daemon=True)
            self._thread.start()

    # --- exporter callbacks ---------------------------------------------------------------------

    def on_remote_id(self, msg):
        if not self.send_drones:
            return
        report = gm.drone_traffic(msg, self.drone_nic)
        if report is not None:
            with self._lock:
                self._pending[('drone', decode.drone_id(msg))] = report

    def on_aircraft(self, msg):
        if not self.send_aircraft:
            return
        report = gm.aircraft_traffic(msg)
        if report is not None:
            with self._lock:
                self._pending[('aircraft', msg.get('type'), msg.get('address'))] = report

    def on_location(self, location_json):
        if self.ownship_sensor is not None and str(location_json.get('sensor ID')) == str(self.ownship_sensor):
            lat, lon = location_json.get('latitude'), location_json.get('longitude')
            if lat is not None and lon is not None:
                self.ownship = (lat, lon, location_json.get('altitude MSL') or 0.0)

    # --- sending ---------------------------------------------------------------------------------

    def _send(self, message):
        data = gm.frame(message)
        for dest in self.destinations:
            try:
                self.sock.sendto(data, dest)
                self.sent += 1
            except OSError as e:
                self.errors += 1
                if self.errors in (1, 10, 100) or self.errors % 1000 == 0:
                    self.log("GDL90: sending to %s:%d failed: %s (%d errors)" % (dest[0], dest[1], e, self.errors))

    def flush(self, now=None):
        """Heartbeat, ID, ownship and the traffic updates since the previous call."""
        with self._lock:
            pending, self._pending = self._pending, {}
        self._send(gm.heartbeat(now, gps_valid=self.ownship is not None, message_count=len(pending)))
        self._send(gm.foreflight_id(name=self.device_name, long_name=self.device_long_name))
        if self.ownship is not None:
            lat, lon, alt = self.ownship
            self._send(gm.ownship(lat, lon, alt))
            self._send(gm.ownship_geo_altitude(alt))
        for report in pending.values():
            self._send(report)
        return len(pending)

    def _loop(self):
        while not self._closed:
            start = time.monotonic()
            try:
                self.flush()
            except Exception as e:                # never let the thread die
                self.errors += 1
                self.log("GDL90: error: %r" % e)
            self._stop.wait(max(0.0, self.interval_s - (time.monotonic() - start)))

    def close(self):
        self._closed = True
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + 1)
        self.sock.close()


def parse_destinations(items):
    out = []
    for d in items:
        if isinstance(d, (tuple, list)):
            out.append((str(d[0]), int(d[1])))
        else:
            host, port = str(d).rsplit(':', 1)
            out.append((host, int(port)))
    return out

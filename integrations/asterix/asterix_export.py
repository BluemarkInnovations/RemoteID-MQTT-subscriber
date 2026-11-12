# (c) Bluemark Innovations BV
# MIT license
"""ASTERIX exporter: decoded DroneScout messages -> ASTERIX over UDP.

Every interval_s (default 1 s) one datagram per category is sent (more if it
doesn't fit in max_datagram bytes) with the newest report of every object seen
since the previous one, to every destination (unicast and/or multicast).
Heartbeats: CAT023 per sensor and CAT065 for the gateway, every heartbeat_s,
also when there is no traffic.

Identification: one SAC for the installation. Each DroneScout sensor gets its own
SIC from the sensor ID -> SIC table (CAT021, CAT023, CAT129); a sensor that is not
in the table is reported once and not sent (no guessed SIC). CAT062 and CAT065 use
the SIC of the gateway.
"""

import ipaddress
import socket
import threading
import time

from modules import decode
from modules import exporters
from integrations.asterix import asterix_map as am

ALL_CATEGORIES = (21, 23, 62, 65, 129)


class AsterixExporter(exporters.Exporter):
    name = "asterix"

    def __init__(self, destinations, sac, sic_table, categories=(21, 23, 129), gateway_sic=None,
                 interval_s=1.0, heartbeat_s=10.0, stale_s=10.0, cat062_aircraft=False,
                 cat129_country='ZZ', cat129_destination=None, cat129_special_purpose=True,
                 range_nm=None, service_id=1, max_datagram=1400, multicast_ttl=1, multicast_interface=None,
                 log=print, clock=time.monotonic, start=True):
        self.destinations = [(str(h), int(p)) for h, p in destinations]
        if not self.destinations:
            raise ValueError("no ASTERIX destinations")
        self.sac = int(sac)
        self.sic_table = {str(k): int(v) for k, v in (sic_table or {}).items()}
        self.categories = set(int(c) for c in categories)
        unknown = self.categories - set(ALL_CATEGORIES)
        if unknown:
            raise ValueError("unsupported ASTERIX categories: %s" % sorted(unknown))
        if self.categories & {62, 65} and gateway_sic is None:
            raise ValueError("CAT062 / CAT065 need the SIC of the gateway (asterix_gateway_sic)")
        self.gateway_sic = gateway_sic
        self.interval_s = interval_s
        self.heartbeat_s = heartbeat_s
        self.stale_s = stale_s
        self.cat062_aircraft = cat062_aircraft
        self.cat129_country = cat129_country
        self.cat129_destination = cat129_destination
        self.cat129_special_purpose = cat129_special_purpose
        self.range_nm = range_nm
        self.service_id = service_id
        self.max_datagram = max_datagram
        self.log = log
        self._clock = clock

        self.sent_datagrams = 0
        self.sent_records = {c: 0 for c in ALL_CATEGORIES}
        self.errors = 0
        self.skipped_unknown_sensor = 0
        self._warned = set()
        self._lock = threading.Lock()
        self._pending = {21: {}, 62: {}, 129: {}}        # key -> message (newest)
        self._status = {}                                # sensor ID -> status message
        self._tracks021 = {}                             # (sensor, key) -> track number
        self._next021 = {}                               # sensor -> next track number
        self._tracks062 = {}                             # key -> [track number, last seen, last msg, kind]
        self._next062 = 1
        self._last_heartbeat = None

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, multicast_ttl)
        if multicast_interface:
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(multicast_interface))
        for host, _ in self.destinations:
            try:
                if ipaddress.ip_address(host).is_multicast:
                    self.log("ASTERIX: multicast to %s (TTL %d)" % (host, multicast_ttl))
            except ValueError:
                pass                                     # host name
        self._closed = False
        self._thread = None
        if start:
            self._thread = threading.Thread(target=self._loop, name='asterix-flush', daemon=True)
            self._thread.start()

    # --- identification ------------------------------------------------------------

    def sic_for(self, sensor_id):
        sic = self.sic_table.get(str(sensor_id))
        if sic is None:
            self.skipped_unknown_sensor += 1
            if sensor_id not in self._warned:
                self._warned.add(sensor_id)
                self.log("ASTERIX: sensor %s has no SIC in asterix_sic_table, its data is not sent" % sensor_id)
        return sic

    # --- exporter callbacks (exporter thread) ---------------------------------------------

    def on_remote_id(self, msg):
        key = decode.drone_id(msg)
        with self._lock:
            if 129 in self.categories and self.sic_for(msg.sensor_id) is not None:
                self._pending[129][(msg.sensor_id, key)] = msg
            if 62 in self.categories:
                self._pending[62][('rid', key)] = ('rid', msg)

    def on_aircraft(self, msg):
        key = '%s-%s' % (msg.get('type'), msg.get('address'))
        with self._lock:
            if 21 in self.categories and msg.get('type') in ('ADSB', 'UAT') \
                    and self.sic_for(msg.sensor_id) is not None:
                self._pending[21][(msg.sensor_id, key)] = msg
            if 62 in self.categories and self.cat062_aircraft:
                self._pending[62][('aircraft', key)] = ('aircraft', msg)

    def on_status(self, status_json):
        sensor_id = status_json.get('sensor ID')
        if sensor_id is None:
            return
        with self._lock:
            self._status[str(sensor_id)] = status_json

    # --- building and sending (flush thread) -----------------------------------------------

    def _track021(self, sensor_id, key):
        tn = self._tracks021.get((sensor_id, key))
        if tn is None:
            tn = self._next021.get(sensor_id, 0)
            self._next021[sensor_id] = (tn + 1) % 4096
            self._tracks021[(sensor_id, key)] = tn
            if len(self._tracks021) > 20000:
                self._tracks021.clear()
        return tn

    def _send(self, datagrams, category, n_records):
        for data in datagrams:
            for dest in self.destinations:
                try:
                    self.sock.sendto(data, dest)
                    self.sent_datagrams += 1
                except OSError as e:
                    self.errors += 1
                    if self.errors in (1, 10, 100) or self.errors % 1000 == 0:
                        self.log("ASTERIX: sending to %s:%d failed: %s (%d errors)" % (dest[0], dest[1], e, self.errors))
        self.sent_records[category] += n_records

    def flush(self, now_wall=None):
        """Send what is pending, the heartbeats when due and CAT062 end-of-track reports.
        Called every interval_s by the flush thread (and directly by tests)."""
        now_wall = time.time() if now_wall is None else now_wall
        now = self._clock()
        with self._lock:
            pending = {c: self._pending[c] for c in self._pending}
            self._pending = {21: {}, 62: {}, 129: {}}
            statuses = dict(self._status)
            heartbeat = self._last_heartbeat is None or now - self._last_heartbeat >= self.heartbeat_s
            if heartbeat:
                self._last_heartbeat = now

        out = []                       # (category, datagrams, number of records)
        # CAT021
        records = []
        for (sensor_id, key), msg in pending[21].items():
            try:
                r = am.cat021_record(msg, self.sac, self.sic_table[str(sensor_id)],
                                     self._track021(sensor_id, key),
                                     am.SERVICE_UAT if msg.get('type') == 'UAT' else am.SERVICE_1090, now_wall)
            except Exception as e:
                self.errors += 1
                self.log("ASTERIX: CAT021 for %s not encoded: %r" % (key, e))
                continue
            if r is not None:
                records.append(r)
        if records:
            out.append((21, am.datablocks(am.CAT021, records, self.max_datagram), len(records)))

        # CAT129
        records = []
        for (sensor_id, key), msg in pending[129].items():
            r = am.cat129_record(msg, self.sac, self.sic_table[str(sensor_id)], self.cat129_country,
                                 self.cat129_destination, now_wall, self.cat129_special_purpose)
            if r is not None:
                records.append(r)
        if records:
            out.append((129, am.raw_datablock(129, records, self.max_datagram), len(records)))

        # CAT062: updates, then end-of-track for tracks not seen within stale_s
        if 62 in self.categories:
            records = []
            for key, (kind, msg) in pending[62].items():
                track = self._tracks062.get(key)
                first = track is None
                if first:
                    track = self._tracks062[key] = [self._next062, now, msg, kind]
                    self._next062 = self._next062 % 65535 + 1
                track[1], track[2] = now, msg
                r = self._cat062(kind, msg, track[0], first, False, now_wall)
                if r is not None:
                    records.append(r)
            for key, (tn, seen, msg, kind) in list(self._tracks062.items()):
                if now - seen >= self.stale_s:
                    del self._tracks062[key]
                    r = self._cat062(kind, msg, tn, False, True, now_wall)
                    if r is not None:
                        records.append(r)
            if records:
                out.append((62, am.datablocks(am.CAT062, records, self.max_datagram), len(records)))

        if heartbeat:
            if 23 in self.categories:
                records = []
                for sensor_id in sorted(set(statuses) | {s for s, _ in pending[21]} | {s for s, _ in pending[129]}):
                    sic = self.sic_table.get(str(sensor_id))
                    if sic is None:
                        continue
                    records += am.cat023_records(self.sac, sic, statuses.get(str(sensor_id)), self.heartbeat_s,
                                                 self.interval_s, self.range_nm, now=now_wall)
                if records:
                    out.append((23, am.datablocks(am.CAT023, records, self.max_datagram), len(records)))
            if 65 in self.categories:
                r = am.cat065_record(self.sac, self.gateway_sic, self.service_id, True, now_wall)
                out.append((65, am.datablocks(am.CAT065, [r], self.max_datagram), 1))

        for category, datagrams, n in out:
            self._send(datagrams, category, n)

    def _cat062(self, kind, msg, track_number, first, last, now_wall):
        try:
            if kind == 'rid':
                return am.cat062_drone_record(msg, self.sac, self.gateway_sic, track_number, self.service_id,
                                              first, last, now_wall)
            return am.cat062_aircraft_record(msg, self.sac, self.gateway_sic, track_number, self.service_id,
                                             first, last, now_wall)
        except Exception as e:
            self.errors += 1
            self.log("ASTERIX: CAT062 track %d not encoded: %r" % (track_number, e))
            return None

    def _loop(self):
        while not self._closed:
            start = time.monotonic()
            try:
                self.flush()
            except Exception as e:              # never let the thread die
                self.errors += 1
                self.log("ASTERIX: flush error: %r" % e)
            time.sleep(max(0.0, self.interval_s - (time.monotonic() - start)))

    def close(self):
        self._closed = True
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + 1)
        try:
            self.flush()                        # what is still pending
        except Exception:
            pass
        self.sock.close()


def parse_destinations(text_or_list):
    """'host:port' strings (or (host, port) tuples) -> [(host, port)]."""
    out = []
    for d in text_or_list:
        if isinstance(d, (tuple, list)):
            out.append((d[0], int(d[1])))
        else:
            host, port = str(d).rsplit(':', 1)
            out.append((host, int(port)))
    return out


# (c) Bluemark Innovations BV
# MIT license
"""Manual fallback position of sensors without (working) GPS.

Only sensors with the LTE add-on send "location" messages (GNSS of the modem, once a
minute). For the others, or while the GPS has no fix, config.py can give a fixed
position per sensor:

    sensor_locations = {
        'ds02261000000948': (52.21852, 6.97486, 30.0),     # latitude, longitude, altitude MSL (m)
    }

While a sensor has no valid GPS position (no location message with a fix for
gps_timeout_s), every message from it makes mqtt_sub hand its manual position to the
outputs as a location message, at most once per interval_s, with "source": "manual".
A valid GPS position always wins: the fallback stops as soon as one arrives.
"""

import math
import threading
import time

GPS_TIMEOUT_S = 180         # the sensor sends its location once a minute
INTERVAL_S = 60


def _number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def valid_fix(location_json):
    """Whether a location message has a usable position (not missing, not 0/0, satellites > 0)."""
    lat, lon = location_json.get('latitude'), location_json.get('longitude')
    if not (_number(lat) and _number(lon)) or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return False
    if lat == 0 and lon == 0:
        return False
    sats = location_json.get('satellites')
    return not (_number(sats) and sats <= 0)


def parse(table):
    """config.sensor_locations -> {sensor ID: (lat, lon, altitude MSL)}; ValueError when wrong."""
    out = {}
    for sensor_id, pos in (table or {}).items():
        try:
            values = [float(x) for x in pos]
        except (TypeError, ValueError):
            raise ValueError("sensor_locations[%r]: expected (latitude, longitude, altitude MSL)" % sensor_id)
        if len(values) == 2:
            values.append(0.0)
        if len(values) != 3 or not (-90 <= values[0] <= 90 and -180 <= values[1] <= 180):
            raise ValueError("sensor_locations[%r]: expected (latitude, longitude, altitude MSL)" % sensor_id)
        out[str(sensor_id)] = tuple(values)
    return out


class SensorLocations:
    def __init__(self, manual=None, gps_timeout_s=GPS_TIMEOUT_S, interval_s=INTERVAL_S, clock=time.time):
        self.manual = parse(manual)
        self.gps_timeout_s = gps_timeout_s
        self.interval_s = interval_s
        self._clock = clock
        self._last_gps = {}
        self._last_manual = {}
        self._lock = threading.Lock()

    def gps(self, location_json):
        """A location message from a sensor. Returns whether it has a valid fix."""
        ok = valid_fix(location_json)
        if ok:
            with self._lock:
                self._last_gps[str(location_json.get('sensor ID'))] = self._clock()
        return ok

    def seen(self, sensor_id):
        """Any message from a sensor. Returns a location message with its manual position when it is due."""
        sensor_id = str(sensor_id)
        pos = self.manual.get(sensor_id)
        if pos is None:
            return None
        now = self._clock()
        with self._lock:
            last_gps = self._last_gps.get(sensor_id)
            if last_gps is not None and now - last_gps < self.gps_timeout_s:
                return None
            last = self._last_manual.get(sensor_id)
            if last is not None and now - last < self.interval_s:
                return None
            self._last_manual[sensor_id] = now
        return {'sensor ID': sensor_id, 'timestamp': int(now * 1000), 'latitude': pos[0], 'longitude': pos[1],
                'altitude MSL': pos[2], 'source': 'manual'}

# (c) Bluemark Innovations BV
# MIT license
"""Alarm logic for the VMS output: turns the stream of detections into a few events
a security operator (and a VMS rule) can act on. No I/O, so it is easy to test.

Events (dicts with 'event' and the fields used in the message templates):

    drone_detected    a drone is seen (again) after not being seen for lost_s
    drone_update      the drone is still there: every update_s while it is (0: off)
    drone_lost        the drone hasn't been seen for lost_s
    aircraft_detected / aircraft_lost     same for aircraft (off by default)
    sensor_degraded / sensor_normal       a sensor's status changed
"""

import math
import time
from datetime import datetime, timezone

from modules import decode


def _f(value, fmt):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ''
    return fmt % value


def _time(epoch_ms):
    if not epoch_ms:
        return ''
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def drone_fields(msg):
    loc = msg.uas.location
    sys_ = msg.uas.system
    pilot = sys_.has_operator_position
    return {
        'kind': 'drone', 'id': decode.drone_id(msg), 'sensor': msg.sensor_id or '',
        'time': _time(msg.timestamp_ms),
        'lat': _f(loc.latitude, '%.6f') if loc.has_position else '',
        'lon': _f(loc.longitude, '%.6f') if loc.has_position else '',
        'height': _f(loc.height, '%.0f') if loc.valid else '',
        'speed': _f(loc.speed_horizontal, '%.1f') if loc.valid else '',
        'pilot_lat': _f(sys_.operator_latitude, '%.6f') if pilot else '',
        'pilot_lon': _f(sys_.operator_longitude, '%.6f') if pilot else '',
        'operator': msg.uas.operator_id.id_text if msg.uas.operator_id.valid else '',
        'rssi': _f(msg.rssi, '%d'), 'transport': msg.type or '',
        'model': msg.extra.get('model') or '' if msg.extra.get('SN present') == 1 else '',
    }


def aircraft_fields(msg):
    lat, lon = msg.get('latitude'), msg.get('longitude')
    has_pos = lat is not None and lon is not None and not (lat == 0 and lon == 0)
    return {
        'kind': 'aircraft', 'id': '%s-%s' % (msg.get('type'), (msg.get('address') or '').upper()),
        'sensor': msg.sensor_id or '', 'time': _time(msg.get('timestamp')),
        'lat': _f(lat, '%.6f') if has_pos else '', 'lon': _f(lon, '%.6f') if has_pos else '',
        'height': _f(msg.get('altitude'), '%.0f'), 'speed': _f(msg.get('speed'), '%.1f'),
        'callsign': (msg.get('flight') or '').strip(), 'rssi': _f(msg.get('RSSI'), '%d'),
        'transport': msg.get('type') or '', 'pilot_lat': '', 'pilot_lon': '', 'operator': '', 'model': '',
    }


class Alarms:
    def __init__(self, lost_s=30.0, update_s=0.0, aircraft=False, clock=time.monotonic):
        self.lost_s = lost_s
        self.update_s = update_s
        self.aircraft = aircraft
        self._clock = clock
        self._objects = {}           # key -> [first seen, last seen, last event time, fields]
        self._sensor_status = {}

    def _seen(self, key, fields):
        now = self._clock()
        obj = self._objects.get(key)
        if obj is None:
            self._objects[key] = [now, now, now, fields]
            return [dict(fields, event='%s_detected' % fields['kind'], duration='0')]
        obj[1], obj[3] = now, fields
        if self.update_s and fields['kind'] == 'drone' and now - obj[2] >= self.update_s:
            obj[2] = now
            return [dict(fields, event='drone_update', duration='%.0f' % (now - obj[0]))]
        return []

    def drone(self, msg):
        return self._seen(('drone', decode.drone_id(msg)), drone_fields(msg))

    def aircraft_message(self, msg):
        if not self.aircraft:
            return []
        return self._seen(('aircraft', msg.get('type'), msg.get('address')), aircraft_fields(msg))

    def status(self, status_json):
        sensor = str(status_json.get('sensor ID') or '')
        status = status_json.get('status')
        if not sensor or status not in ('normal', 'degraded'):
            return []
        previous = self._sensor_status.get(sensor)
        self._sensor_status[sensor] = status
        if previous == status or (previous is None and status == 'normal'):
            return []                 # first "normal" is not news; a first "degraded" is
        return [{'event': 'sensor_%s' % status, 'kind': 'sensor', 'id': sensor, 'sensor': sensor,
                 'details': status_json.get('status details') or '', 'time': _time(status_json.get('timestamp'))}]

    def tick(self):
        """Lost events for objects not seen within lost_s; call about once per second."""
        now = self._clock()
        out = []
        for key, (first, last, _, fields) in list(self._objects.items()):
            if now - last >= self.lost_s:
                del self._objects[key]
                out.append(dict(fields, event='%s_lost' % fields['kind'], duration='%.0f' % (last - first)))
        return out

    @property
    def active(self):
        return len(self._objects)


class _Default(dict):
    def __missing__(self, key):
        return ''


def render(template, event):
    """str.format with unknown fields as ''."""
    return template.format_map(_Default(event))

# (c) Bluemark Innovations BV
# MIT license
"""Cursor-on-Target (CoT) events from decoded DroneScout messages.

Pure functions: decoded message in, CoT XML (bytes) out, so they are easy to
test. Every function returns None when there is nothing to show, e.g. an
object without a position (TAK needs a point).

CoT basics used here: one <event> per object, a fixed uid per object so TAK
moves the marker instead of adding one, and a 'stale' time after which TAK
removes the marker by itself.
"""

import math
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from modules import decode
from modules import open_drone_id
from modules import aircraft

UNKNOWN = 9999999.0     # CoT value for unknown hae / ce / le

DEFAULTS = {
    'stale_s': 10.0,                 # drones, pilots, aircraft
    'sensor_stale_s': 900.0,         # DroneScout sensors
    'type_drone': 'a-u-A-M-F-Q',     # unknown, air, military, fixed wing, UAV (common for drones in TAK)
    'type_operator': 'a-u-G',        # unknown, ground
    'type_sensor': 'a-f-G-E-S',      # friendly, ground, equipment, sensor
    'type_aircraft': 'a-n-A-C',      # neutral, air, civil
    'type_aircraft_fixed_wing': 'a-n-A-C-F',
    'type_aircraft_rotorcraft': 'a-n-A-C-H',
    'type_aircraft_lighter_than_air': 'a-n-A-C-L',
}

# RemoteID accuracy codes -> meters
_HORIZ_ACCURACY_M = {1: 18520.0, 2: 7408.0, 3: 3704.0, 4: 1852.0, 5: 926.0, 6: 555.6,
                     7: 185.2, 8: 92.6, 9: 30.0, 10: 10.0, 11: 3.0, 12: 1.0}
_VERT_ACCURACY_M = {1: 150.0, 2: 45.0, 3: 25.0, 4: 10.0, 5: 3.0, 6: 1.0}


def _settings(settings):
    s = dict(DEFAULTS)
    if settings:
        s.update({k: v for k, v in settings.items() if v is not None})
    return s


def cot_time(epoch_s):
    """CoT time format, UTC with milliseconds: 2026-11-03T13:27:51.226Z"""
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def _known(value):
    return value is not None and not (isinstance(value, float) and math.isnan(value))


def _num(value):
    """Short number formatting for XML attributes."""
    if isinstance(value, float):
        return ('%.8f' % value).rstrip('0').rstrip('.')
    return str(value)


def _event(uid, cot_type, lat, lon, hae, ce, le, timestamp_s, stale_s, now=None):
    """<event> with <point> and an empty <detail>.

    time/start: when the sensor saw the object. stale: counted from the
    gateway clock, so a sensor with a wrong clock doesn't make TAK hide
    the marker straight away."""
    now = time.time() if now is None else now
    t = cot_time(timestamp_s if timestamp_s else now)
    event = ET.Element('event', {
        'version': '2.0', 'uid': uid, 'type': cot_type, 'how': 'm-g',
        'time': t, 'start': t, 'stale': cot_time(now + stale_s),
    })
    ET.SubElement(event, 'point', {
        'lat': _num(lat), 'lon': _num(lon),
        'hae': _num(hae if _known(hae) else UNKNOWN),
        'ce': _num(ce if _known(ce) else UNKNOWN),
        'le': _num(le if _known(le) else UNKNOWN),
    })
    detail = ET.SubElement(event, 'detail')
    return event, detail


def _to_bytes(event):
    return ET.tostring(event, encoding='utf-8')


def _track(detail, speed_ms, course_deg):
    """Speed/course; left out when unknown, or 0/0 (no velocity message received yet)."""
    if not _known(speed_ms) or not _known(course_deg):
        return
    if speed_ms == 0 and course_deg == 0:
        return
    ET.SubElement(detail, 'track', {'speed': _num(float(speed_ms)), 'course': _num(float(course_deg))})


# --- RemoteID ---------------------------------------------------------------

def drone_id(msg):
    """Stable ID of a drone: serial number, else another valid basic ID, else the MAC address."""
    return decode.drone_id(msg)


def drone_uid(msg):
    return 'dronescout-rid-' + drone_id(msg)


def _rid_remarks(msg):
    uas = msg.uas
    parts = ['RemoteID %s' % (msg.type or '')]
    for b in uas.basic_id:
        if b.valid:
            value = b.id_text if b.is_text else b.id_raw.rstrip(b'\x00').hex()
            parts.append('%s: %s' % (open_drone_id.decode_basicID_ID_type(b.id_type) or 'ID', value))
    if uas.basic_id[0].valid:
        parts.append('UA type: %s' % open_drone_id.decode_basicID_UA_type(uas.basic_id[0].ua_type))
    if uas.location.valid:
        parts.append('status: %s' % open_drone_id.decode_location_status(uas.location.status))
    if uas.system.valid and uas.system.classification_type == 1:
        parts.append('EU %s, %s' % (open_drone_id.decode_system_ua_category(uas.system.category_eu),
                                    open_drone_id.decode_system_ua_class(uas.system.class_eu)))
    if uas.operator_id.valid and uas.operator_id.id_text:
        parts.append('operator ID: %s' % uas.operator_id.id_text)
    if uas.self_id.valid:
        text = uas.self_id.desc_raw.split(b'\x00')[0].decode('ascii', 'replace').strip()
        if text:
            parts.append('self-ID: %s' % text)
    extra = msg.extra
    if extra.get('SN present') == 1 and extra.get('manufacturer'):
        parts.append('%s %s' % (extra.get('manufacturer'), extra.get('model') or ''))
    if msg.rssi is not None:
        parts.append('RSSI %s dBm' % msg.rssi)
    if msg.sensor_id:
        parts.append('sensor %s' % msg.sensor_id)
    return ', '.join(p.strip() for p in parts)


def drone_event(msg, settings=None, now=None):
    """CoT event for the drone of a RemoteIDMessage, None without a position."""
    s = _settings(settings)
    loc = msg.uas.location
    if not loc.has_position:
        return None
    timestamp_s = msg.timestamp_ms / 1000.0 if msg.timestamp_ms else None
    event, detail = _event(drone_uid(msg), s['type_drone'], loc.latitude, loc.longitude,
                           loc.altitude_geo, _HORIZ_ACCURACY_M.get(loc.horiz_accuracy),
                           _VERT_ACCURACY_M.get(loc.vert_accuracy), timestamp_s, s['stale_s'], now)
    ET.SubElement(detail, 'contact', {'callsign': drone_id(msg)})
    _track(detail, loc.speed_horizontal, loc.direction)
    ET.SubElement(detail, 'remarks').text = _rid_remarks(msg)
    return _to_bytes(event)


def operator_event(msg, settings=None, now=None):
    """CoT event for the pilot / operator position, None when it isn't known."""
    s = _settings(settings)
    sys_ = msg.uas.system
    if not sys_.has_operator_position:
        return None
    timestamp_s = msg.timestamp_ms / 1000.0 if msg.timestamp_ms else None
    event, detail = _event('dronescout-op-' + drone_id(msg), s['type_operator'],
                           sys_.operator_latitude, sys_.operator_longitude, sys_.operator_altitude_geo,
                           None, None, timestamp_s, s['stale_s'], now)
    ET.SubElement(detail, 'contact', {'callsign': 'pilot ' + drone_id(msg)})
    ET.SubElement(detail, 'link', {'uid': drone_uid(msg), 'type': s['type_drone'], 'relation': 'p-p'})
    remarks = 'pilot of %s, location type: %s' % (
        drone_id(msg), open_drone_id.decode_system_operator_location_type(sys_.operator_location_type))
    if msg.uas.operator_id.valid and msg.uas.operator_id.id_text:
        remarks += ', operator ID: %s' % msg.uas.operator_id.id_text
    ET.SubElement(detail, 'remarks').text = remarks
    return _to_bytes(event)


# --- ADS-B / UAT / ADS-L / FLARM / OGN --------------------------------------

def aircraft_type(msg, settings=None):
    """CoT type from the emitter category (ADS-B / UAT categories)."""
    s = _settings(settings)
    if msg.get('type') not in ('ADSB', 'UAT'):
        return s['type_aircraft']
    try:
        category = int(msg.get('category'))
    except (TypeError, ValueError):
        return s['type_aircraft']
    if category in (1, 2, 3, 4, 5, 6, 9, 12):    # weight classes, highly maneuverable, glider, ultralight
        return s['type_aircraft_fixed_wing']
    if category == 7:
        return s['type_aircraft_rotorcraft']
    if category == 10:
        return s['type_aircraft_lighter_than_air']
    if category == 14:                          # UAV with ADS-B
        return s['type_drone']
    return s['type_aircraft']


def aircraft_uid(msg):
    address = (msg.get('address') or '').upper()
    if msg.get('type') in ('ADSB', 'UAT') and msg.get('address type') in (0, 2):   # ICAO address
        return 'icao-' + address
    technology = '-'.join((msg.get('type') or 'aircraft').lower().split())   # no spaces in a uid
    return 'dronescout-%s-%s' % (technology, address)


def aircraft_event(msg, settings=None, now=None):
    """CoT event for an AircraftMessage, None without a position."""
    s = _settings(settings)
    try:
        lat = float(msg.get('latitude'))
        lon = float(msg.get('longitude'))
    except (TypeError, ValueError):
        return None
    if lat == 0.0 and lon == 0.0:
        return None
    timestamp_s = msg.get('timestamp') / 1000.0 if msg.get('timestamp') else None
    # baro altitude is not a WGS84 height; it is still the best estimate TAK gets
    event, detail = _event(aircraft_uid(msg), aircraft_type(msg, s), lat, lon, msg.get('altitude'),
                           None, None, timestamp_s, s['stale_s'], now)
    flight = (msg.get('flight') or '').strip()
    ET.SubElement(detail, 'contact', {'callsign': flight or (msg.get('address') or '').upper()})
    _track(detail, msg.get('speed'), msg.get('track'))

    parts = ['%s %s MHz' % (msg.get('type'), msg.get('frequency MHz')),
             'address %s (%s)' % (msg.get('address'), aircraft.decode_address_type(msg.get('address type') or 0))]
    if flight:
        parts.append('flight %s' % flight)
    if msg.get('category') is not None:
        parts.append('category: %s' % aircraft.decode_emitter_category(msg.get('category'), msg.get('type')))
    if msg.get('altitude') is not None:
        parts.append('altitude %s m (%s)' % (msg.get('altitude'), aircraft.decode_altitude_type(msg.get('altitude type') or 0)))
    if msg.get('flight state') is not None:
        parts.append('%s' % aircraft.decode_flight_state(msg.get('flight state')))
    if msg.get('emergency status'):
        parts.append('EMERGENCY')
    if msg.get('RSSI') is not None:
        parts.append('RSSI %s dBm' % msg.get('RSSI'))
    if msg.sensor_id:
        parts.append('sensor %s' % msg.sensor_id)
    ET.SubElement(detail, 'remarks').text = ', '.join(parts)
    return _to_bytes(event)


# --- DroneScout sensors -----------------------------------------------------

def sensor_event(sensor_id, position, status_json=None, settings=None, now=None):
    """CoT event for a sensor. position = (lat, lon, altitude MSL or None)."""
    s = _settings(settings)
    if position is None:
        return None
    lat, lon = position[0], position[1]
    alt = position[2] if len(position) > 2 else None
    timestamp_s = None
    if status_json and status_json.get('timestamp'):
        timestamp_s = status_json.get('timestamp') / 1000.0
    # altitude is MSL, not WGS84: close enough for a ground marker, so no le given
    event, detail = _event('dronescout-' + str(sensor_id), s['type_sensor'], lat, lon, alt,
                           None, None, timestamp_s, s['sensor_stale_s'], now)
    ET.SubElement(detail, 'contact', {'callsign': 'DroneScout ' + str(sensor_id)})
    parts = ['DroneScout sensor %s' % sensor_id]
    if status_json:
        parts.append('status: %s' % status_json.get('status'))
        if status_json.get('status details'):
            parts.append('details: %s' % status_json.get('status details'))
        for key in ('WLAN radios', 'SDR receivers'):
            if isinstance(status_json.get(key), dict):
                parts.append('%s: %s' % (key, ', '.join('%s %s' % kv for kv in status_json[key].items())))
        if status_json.get('model'):
            parts.append('%s %s' % (status_json.get('model'), status_json.get('firmware version') or ''))
    ET.SubElement(detail, 'remarks').text = ', '.join(p.strip() for p in parts)
    return _to_bytes(event)


def presence_event(uid, callsign, position=None, stale_s=3600.0, now=None):
    """The gateway announcing itself, sent first on every TCP/TLS connection.

    ATAK, WinTAK and iTAK do the same. FreeTAKServer requires it: it takes the
    first event of a connection as the identity of the client and closes the
    connection when that isn't a usable presence event. TAK Server lists the
    gateway as a contact. position = (lat, lon[, hae]) or None (0, 0)."""
    now = time.time() if now is None else now
    lat, lon = (position[0], position[1]) if position else (0.0, 0.0)
    hae = position[2] if position and len(position) > 2 else None
    event, detail = _event(uid, 'a-f-G-U-C', lat, lon, hae, None, None, now, stale_s, now)
    event.set('how', 'h-e')
    ET.SubElement(detail, 'takv', {'platform': 'DroneScout gateway', 'device': 'gateway',
                                   'os': 'linux', 'version': '1'})
    ET.SubElement(detail, 'contact', {'callsign': callsign})
    ET.SubElement(detail, '__group', {'name': 'Cyan', 'role': 'Team Member'})
    ET.SubElement(detail, 'remarks').text = 'DroneScout gateway'
    return _to_bytes(event)


def ping_event(uid, now=None):
    """TAK keep-alive (t-x-c-t), sent when the connection has been idle."""
    now = time.time() if now is None else now
    event, _ = _event(uid + '-ping', 't-x-c-t', 0.0, 0.0, 0.0, None, None, now, 20.0, now)
    event.set('how', 'h-g-i-g-o')
    return _to_bytes(event)

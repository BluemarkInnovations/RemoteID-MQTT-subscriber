# (c) Bluemark Innovations BV
# MIT license
"""Plain JSON events from decoded DroneScout messages, for the REST and UDP outputs.

Pure functions: decoded message in, dict out. Unknown values are left out (no
NaN, no null), units are SI and in the field names. The schema is described in
integrations/rest/README.md; "schema_version" changes when a field changes meaning.
"""

import math
from datetime import datetime, timezone

from modules import decode
from modules import open_drone_id
from modules import aircraft as aircraft_mod

SCHEMA_VERSION = 1

_HORIZ_ACCURACY_M = {1: 18520.0, 2: 7408.0, 3: 3704.0, 4: 1852.0, 5: 926.0, 6: 555.6,
                     7: 185.2, 8: 92.6, 9: 30.0, 10: 10.0, 11: 3.0, 12: 1.0}
_VERT_ACCURACY_M = {1: 150.0, 2: 45.0, 3: 25.0, 4: 10.0, 5: 3.0, 6: 1.0}


def iso_time(epoch_s):
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def _clean(d):
    """Leave out unknown values (None, NaN, '') recursively."""
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = _clean(v)
            if not v:
                continue
        if v is None or v == '' or (isinstance(v, float) and math.isnan(v)):
            continue
        out[k] = v
    return out


def _time(timestamp_ms):
    return iso_time(timestamp_ms / 1000.0) if timestamp_ms else None


def drone_event(msg):
    """Event for a RemoteIDMessage (also without a position: the ID is useful on its own)."""
    uas = msg.uas
    loc = uas.location
    sys_ = uas.system
    event = {
        'type': 'drone',
        'id': decode.drone_id(msg),
        'sensor_id': msg.sensor_id,
        'time': _time(msg.timestamp_ms),
        'transport': msg.type,
        'rssi_dbm': msg.rssi,
        'channel': msg.channel or None,
        'mac_address': msg.mac_address,
        'serial_number': uas.serial_number,
    }
    for b in uas.basic_id:
        if b.valid and b.id_type not in (0, 1):
            event['uas_id'] = b.id_text if b.is_text else b.id_raw.rstrip(b'\x00').hex()
            event['uas_id_type'] = open_drone_id.decode_basicID_ID_type(b.id_type)
    if uas.basic_id[0].valid:
        event['ua_type'] = open_drone_id.decode_basicID_UA_type(uas.basic_id[0].ua_type)
    if loc.valid:
        event.update({
            'status': open_drone_id.decode_location_status(loc.status),
            'latitude': loc.latitude,
            'longitude': loc.longitude,
            'altitude_geodetic_m': loc.altitude_geo,       # WGS-84 ellipsoid
            'altitude_pressure_m': loc.altitude_baro,
            'height_m': loc.height,
            'height_reference': ('ground' if loc.height_type == 1 else 'takeoff') if not math.isnan(loc.height) else None,
            'speed_mps': loc.speed_horizontal,
            'track_deg': loc.direction,
            'vertical_speed_mps': loc.speed_vertical,
            'horizontal_accuracy_m': _HORIZ_ACCURACY_M.get(loc.horiz_accuracy),
            'vertical_accuracy_m': _VERT_ACCURACY_M.get(loc.vert_accuracy),
        })
    operator = {}
    if uas.operator_id.valid:
        operator['id'] = uas.operator_id.id_text
    if sys_.has_operator_position:
        operator.update({
            'latitude': sys_.operator_latitude,
            'longitude': sys_.operator_longitude,
            'altitude_geodetic_m': sys_.operator_altitude_geo,
            'location_type': open_drone_id.decode_system_operator_location_type(sys_.operator_location_type),
        })
    event['operator'] = operator
    if sys_.valid and sys_.classification_type == 1:
        event['eu_category'] = open_drone_id.decode_system_ua_category(sys_.category_eu)
        event['eu_class'] = open_drone_id.decode_system_ua_class(sys_.class_eu)
    if uas.self_id.valid:
        event['self_id'] = uas.self_id.desc_raw.split(b'\x00')[0].decode('ascii', 'replace').strip()
    if msg.extra.get('SN present') == 1:
        event['manufacturer'] = msg.extra.get('manufacturer')
        event['model'] = msg.extra.get('model')
    return _clean(event)


def aircraft_event(msg):
    """Event for an AircraftMessage (ADS-B, UAT, ADS-L, FLARM, OGN)."""
    flight = (msg.get('flight') or '').strip()
    alt_type = msg.get('altitude type')
    event = {
        'type': 'aircraft',
        'id': '%s-%s' % (msg.get('type'), (msg.get('address') or '').upper()),
        'sensor_id': msg.sensor_id,
        'time': _time(msg.get('timestamp')),
        'technology': msg.get('type'),
        'frequency_mhz': msg.get('frequency MHz'),
        'rssi_dbm': msg.get('RSSI'),
        'address': (msg.get('address') or '').upper(),
        'address_type': aircraft_mod.decode_address_type(msg.get('address type'))
        if msg.get('address type') is not None else None,
        'callsign': flight,
        'emitter_category': aircraft_mod.decode_emitter_category(msg.get('category'), msg.get('type'))
        if msg.get('category') is not None else None,
        'latitude': msg.get('latitude'),
        'longitude': msg.get('longitude'),
        'altitude_m': msg.get('altitude'),
        'altitude_type': aircraft_mod.decode_altitude_type(alt_type) if alt_type is not None else None,
        'speed_mps': msg.get('speed'),
        'track_deg': msg.get('track'),
        'vertical_rate_mps': msg.get('vertical rate'),
        'on_ground': {0: True, 1: False}.get(msg.get('flight state')),
        'emergency': bool(msg.get('emergency status')) if msg.get('emergency status') is not None else None,
    }
    if event['latitude'] == 0 and event['longitude'] == 0:
        event['latitude'] = event['longitude'] = None
    return _clean(event)


def status_event(status_json):
    return _clean({
        'type': 'sensor_status',
        'sensor_id': status_json.get('sensor ID'),
        'time': _time(status_json.get('timestamp')),
        'status': status_json.get('status'),
        'status_details': status_json.get('status details'),
        'model': status_json.get('model'),
        'firmware_version': status_json.get('firmware version'),
        'wlan_radios': status_json.get('WLAN radios') if isinstance(status_json.get('WLAN radios'), dict) else None,
        'sdr_receivers': status_json.get('SDR receivers') if isinstance(status_json.get('SDR receivers'), dict) else None,
    })


def location_event(location_json):
    return _clean({
        'type': 'sensor_location',
        'sensor_id': location_json.get('sensor ID'),
        'time': _time(location_json.get('timestamp')),
        'latitude': location_json.get('latitude'),
        'longitude': location_json.get('longitude'),
        'altitude_msl_m': location_json.get('altitude MSL'),
        'satellites': location_json.get('satellites'),
        'hdop': location_json.get('HDOP'),
        'source': location_json.get('source'),          # 'manual': config.sensor_locations, else GPS
    })


def envelope(events, source, sent_epoch_s):
    return {'schema_version': SCHEMA_VERSION, 'source': source, 'sent': iso_time(sent_epoch_s), 'events': events}

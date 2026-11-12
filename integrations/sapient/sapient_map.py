# (c) Bluemark Innovations BV
# MIT license
"""SAPIENT (BSI Flex 335 v2.0) messages from decoded DroneScout messages.

Pure functions: decoded message in, SapientMessage out, so they are easy to
test. The TCP side (registration handshake, heartbeats, pacing) is in
sapient_export.py.

Identifiers, in the formats the Dstl Apex middleware checks strictly:
- node_id: UUID, version 4 format, lower case. One SAPIENT node per DroneScout
  sensor, derived from the sensor ID, so it stays the same after a restart.
- report_id / object_id / task_id: ULID (26 characters, Crockford base32).
"""

import hashlib
import math
import os
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)      # the generated classes import "sapient_msg.bsi_flex_335_v2_0..."

from google.protobuf.timestamp_pb2 import Timestamp                       # noqa: E402
from sapient_msg.bsi_flex_335_v2_0 import sapient_message_pb2 as sm       # noqa: E402
from sapient_msg.bsi_flex_335_v2_0 import registration_pb2 as reg         # noqa: E402
from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2 as st         # noqa: E402
from sapient_msg.bsi_flex_335_v2_0 import detection_report_pb2 as det     # noqa: E402
from sapient_msg.bsi_flex_335_v2_0 import task_ack_pb2 as tack            # noqa: E402
from sapient_msg.bsi_flex_335_v2_0 import location_pb2 as loc             # noqa: E402
from sapient_msg.bsi_flex_335_v2_0 import velocity_pb2 as vel             # noqa: E402

from modules import decode                    # noqa: E402
from modules import open_drone_id             # noqa: E402
from modules import aircraft as aircraft_mod  # noqa: E402

ICD_VERSION = 'BSI Flex 335 v2.0'
MODE = 'default'

LATLON = loc.LOCATION_COORDINATE_SYSTEM_LAT_LNG_DEG_M
WGS84_E = loc.LOCATION_DATUM_WGS84_E      # ellipsoid: RemoteID geodetic altitude, GNSS height
WGS84_G = loc.LOCATION_DATUM_WGS84_G      # geoid (~MSL): GNSS MSL, barometric altitude, sensor MSL

DEFAULTS = {
    'name': 'DroneScout',
    'manufacturer': 'BlueMark Innovations',
    'model': 'DroneScout',
    'heartbeat_s': 10.0,
    'coverage_m': 5000.0,              # radius around the sensor in StatusReport.coverage, 0 = leave out
    # classification: (class, sub-class or None). Check these names against the taxonomy of
    # BSI Flex 335 v2.0 section 7 (or the fusion node's run-time taxonomy) before deployment.
    'class_drone': ('Air Vehicle', 'UAV'),
    'class_fixed_wing': ('Air Vehicle', 'Fixed Wing'),
    'class_rotorcraft': ('Air Vehicle', 'Rotary Wing'),
    'class_lighter_than_air': ('Air Vehicle', 'Lighter Than Air'),
    'class_aircraft': ('Air Vehicle', None),
}

# RemoteID accuracy codes -> meters (95 %)
_HORIZ_ACCURACY_M = {1: 18520.0, 2: 7408.0, 3: 3704.0, 4: 1852.0, 5: 926.0, 6: 555.6,
                     7: 185.2, 8: 92.6, 9: 30.0, 10: 10.0, 11: 3.0, 12: 1.0}
_VERT_ACCURACY_M = {1: 150.0, 2: 45.0, 3: 25.0, 4: 10.0, 5: 3.0, 6: 1.0}

# object_info fields per source, declared in the registration (type, units)
OBJECT_INFO_RID = [
    ('source', 'RemoteID transport, e.g. WLAN Beacon, BT5 long range'),
    ('serial number', 'ANSI/CTA-2063-A'),
    ('UAS ID', 'text or hex'),
    ('UAS ID type', 'text'),
    ('UA type', 'text'),
    ('status', 'text'),
    ('operator ID', 'text'),
    ('operator latitude', 'degrees WGS84'),
    ('operator longitude', 'degrees WGS84'),
    ('operator altitude', 'metres WGS84 ellipsoid'),
    ('operator location type', 'text'),
    ('EU category', 'text'),
    ('EU class', 'text'),
    ('self-ID', 'text'),
    ('height', 'metres'),
    ('height type', 'text'),
    ('MAC address', 'hex'),
    ('manufacturer', 'text'),
    ('model', 'text'),
]
OBJECT_INFO_AIRCRAFT = [
    ('source', 'ADSB, UAT, ADSL, FLARM, OGN'),
    ('address', 'hex'),
    ('ICAO address', 'hex'),
    ('address type', 'text'),
    ('callsign', 'text'),
    ('emitter category', 'text'),
    ('altitude type', 'text'),
    ('flight state', 'text'),
    ('emergency', 'true/false'),
]


def _settings(settings):
    s = dict(DEFAULTS)
    if settings:
        s.update({k: v for k, v in settings.items() if v is not None})
    return s


# --- identifiers ----------------------------------------------------------------

_CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'


def new_ulid(epoch_s=None):
    """ULID: 48 bit milliseconds + 80 random bits, 26 characters Crockford base32."""
    ms = int((time.time() if epoch_s is None else epoch_s) * 1000) & ((1 << 48) - 1)
    value = (ms << 80) | int.from_bytes(os.urandom(10), 'big')
    out = []
    for _ in range(26):
        out.append(_CROCKFORD[value & 31])
        value >>= 5
    return ''.join(reversed(out))


def node_uuid(sensor_id, namespace='dronescout'):
    """Stable node_id for a sensor: hash of the sensor ID, formatted as a UUID version 4
    (strict validators such as Apex only accept version 4)."""
    digest = bytearray(hashlib.sha256(('%s:%s' % (namespace, sensor_id)).encode()).digest()[:16])
    digest[6] = (digest[6] & 0x0F) | 0x40      # version 4
    digest[8] = (digest[8] & 0x3F) | 0x80      # RFC 4122 variant
    return str(uuid.UUID(bytes=bytes(digest)))


def _known(value):
    return value is not None and not (isinstance(value, float) and math.isnan(value))


def _ts(epoch_s):
    t = Timestamp()
    t.FromNanoseconds(int(epoch_s * 1e9))
    return t


def envelope(node_id, now=None):
    """SapientMessage with timestamp (send time) and node_id; fill in one content field."""
    m = sm.SapientMessage()
    m.timestamp.CopyFrom(_ts(time.time() if now is None else now))
    m.node_id = node_id
    return m


# --- registration -----------------------------------------------------------------

def _duration(seconds):
    d = reg.Registration.Duration()
    d.units = reg.Registration.TIME_UNITS_SECONDS
    d.value = float(seconds)
    return d


def _class_definitions(s):
    """Class -> sub-classes, from the classification settings."""
    classes = {}
    for key in ('class_drone', 'class_fixed_wing', 'class_rotorcraft', 'class_lighter_than_air', 'class_aircraft'):
        cls, sub = s[key]
        classes.setdefault(cls, [])
        if sub and sub not in classes[cls]:
            classes[cls].append(sub)
    out = []
    for cls, subs in classes.items():
        cd = reg.Registration.ClassDefinition(type=cls, units='probability 0-1')
        for sub in subs:
            cd.sub_class.add(type=sub, units='probability 0-1', level=1)
        out.append(cd)
    return out


def registration(node_id, sensor_id, status_json=None, settings=None, now=None):
    """Registration of one DroneScout sensor as a passive RF edge node (ASM)."""
    s = _settings(settings)
    m = envelope(node_id, now)
    r = m.registration
    r.icd_version = ICD_VERSION
    r.name = '%s %s' % (s['name'], sensor_id)
    r.short_name = str(sensor_id)[-12:]
    nd = r.node_definition.add(node_type=reg.Registration.NODE_TYPE_PASSIVE_RF)
    nd.node_sub_type.extend(['RemoteID receiver', 'ADS-B receiver'])

    for category, ctype, value, units in (
            ('RemoteID', 'technologies', 'WLAN Beacon, WLAN NaN, Bluetooth 4 legacy, Bluetooth 5 long range', ''),
            ('RemoteID', 'standards', 'ASTM F3411, ASD-STAN EN 4709-002', ''),
            ('Aircraft', 'technologies', 'ADS-B 1090 MHz, UAT 978 MHz, ADS-L, FLARM, OGN', ''),
            ('Detection', 'range', '%g' % s['coverage_m'], 'm')):
        c = r.capabilities.add(category=category, type=ctype, value=value)
        if units:
            c.units = units

    sd = r.status_definition
    sd.status_interval.CopyFrom(_duration(s['heartbeat_s']))
    sd.location_definition.location_units = LATLON
    sd.location_definition.location_datum = WGS84_G
    sd.coverage_definition.location_units = LATLON
    sd.coverage_definition.location_datum = WGS84_G
    sd.status_report.add(category=reg.Registration.STATUS_REPORT_CATEGORY_STATUS, type='sensor status',
                         units='normal, degraded', on_change=True)
    sd.status_report.add(category=reg.Registration.STATUS_REPORT_CATEGORY_SENSOR, type='receivers',
                         units='text', on_change=True)

    md = r.mode_definition.add(mode_name=MODE, mode_type=reg.Registration.MODE_TYPE_PERMANENT,
                               mode_description='passive reception of RemoteID and aircraft broadcasts')
    md.settle_time.CopyFrom(_duration(0))
    md.maximum_latency.CopyFrom(_duration(2))
    md.scan_type = reg.Registration.SCAN_TYPE_FIXED
    md.tracking_type = reg.Registration.TRACKING_TYPE_TRACK_WITH_RE_ID   # broadcast IDs: tracks re-associate

    dd = md.detection_definition.add()
    dd.location_type.location_units = LATLON
    dd.location_type.location_datum = WGS84_E
    dd.velocity_type.enu_velocity_units.east_north_rate_units = vel.SPEED_UNITS_MS
    dd.velocity_type.enu_velocity_units.up_rate_units = vel.SPEED_UNITS_MS
    dd.velocity_type.location_datum = WGS84_E
    seen = set()
    for ftype, units in OBJECT_INFO_RID + OBJECT_INFO_AIRCRAFT:
        if ftype not in seen:
            seen.add(ftype)
            dd.detection_report.add(category=reg.Registration.DETECTION_REPORT_CATEGORY_OBJECT,
                                    type=ftype, units=units)
    dd.detection_report.add(category=reg.Registration.DETECTION_REPORT_CATEGORY_SIGNAL,
                            type='amplitude', units='dBm')
    dd.detection_report.add(category=reg.Registration.DETECTION_REPORT_CATEGORY_SIGNAL,
                            type='centre_frequency', units='MHz')
    dd.detection_report.add(category=reg.Registration.DETECTION_REPORT_CATEGORY_TRACK,
                            type='state', units='lost')
    dcd = dd.detection_class_definition.add(
        confidence_definition=reg.Registration.CONFIDENCE_DEFINITION_SINGLE_CLASS)
    dcd.class_definition.extend(_class_definitions(s))

    # tasking is not supported (every Task is answered with TaskAck REJECTED), but the
    # task definition is mandatory: declare the region types without commands
    rd = md.task.region_definition
    rd.region_type.append(reg.Registration.REGION_TYPE_AREA_OF_INTEREST)
    area = rd.region_area.add()
    area.location_units = LATLON
    area.location_datum = WGS84_G
    md.task.concurrent_tasks = 0

    cfg = r.config_data.add(manufacturer=s['manufacturer'], model=s['model'])
    cfg.serial_number = str(sensor_id)
    if status_json:
        if status_json.get('model'):
            cfg.model = '%s %s' % (s['model'], status_json.get('model'))
        if status_json.get('firmware version'):
            cfg.software_version = str(status_json.get('firmware version'))
    return m


# --- status report ------------------------------------------------------------------

def _location(lat, lon, alt=None, datum=WGS84_E, x_error=None, y_error=None, z_error=None):
    lo = loc.Location(x=float(lon), y=float(lat), coordinate_system=LATLON, datum=datum)
    if _known(alt):
        lo.z = float(alt)
    for name, value in (('x_error', x_error), ('y_error', y_error), ('z_error', z_error)):
        if _known(value):
            setattr(lo, name, float(value))
    return lo


def _circle(lat, lon, radius_m, points=16):
    """Polygon approximating a circle, for StatusReport.coverage."""
    ll = loc.LocationList()
    for i in range(points):
        a = 2 * math.pi * i / points
        dlat = radius_m * math.cos(a) / 111320.0
        dlon = radius_m * math.sin(a) / (111320.0 * max(math.cos(math.radians(lat)), 1e-6))
        ll.locations.append(_location(lat + dlat, lon + dlon, datum=WGS84_G))
    return ll


def status_report(node_id, status_json=None, position=None, info_new=True, goodbye=False,
                  settings=None, now=None):
    """Heartbeat. status_json: last sensor "status" message (or None when none was received
    yet); position: (lat, lon[, altitude MSL]) or None."""
    s = _settings(settings)
    m = envelope(node_id, now)
    r = m.status_report
    r.report_id = new_ulid(now)
    r.mode = MODE
    r.info = st.StatusReport.INFO_NEW if info_new else st.StatusReport.INFO_UNCHANGED
    sensor_status = (status_json or {}).get('status')
    if goodbye:
        r.system = st.StatusReport.SYSTEM_GOODBYE
    elif status_json is None:
        r.system = st.StatusReport.SYSTEM_WARNING        # no status message from the sensor yet
    elif sensor_status in ('normal', 'ok'):
        r.system = st.StatusReport.SYSTEM_OK
    elif sensor_status == 'degraded':
        r.system = st.StatusReport.SYSTEM_WARNING
    else:
        r.system = st.StatusReport.SYSTEM_ERROR

    if position is not None:
        alt = position[2] if len(position) > 2 else None
        r.node_location.CopyFrom(_location(position[0], position[1], alt, datum=WGS84_G))
        if s['coverage_m']:
            r.coverage.add().location_list.CopyFrom(_circle(position[0], position[1], s['coverage_m']))

    if status_json is None:
        r.status.add(status_level=st.StatusReport.STATUS_LEVEL_WARNING_STATUS,
                     status_type=st.StatusReport.STATUS_TYPE_OTHER,
                     status_value='no status message received from the sensor yet')
    else:
        if sensor_status not in ('normal', 'ok'):
            r.status.add(status_level=st.StatusReport.STATUS_LEVEL_WARNING_STATUS,
                         status_type=st.StatusReport.STATUS_TYPE_INTERNAL_FAULT,
                         status_value='sensor status %s: %s' % (sensor_status,
                                                                status_json.get('status details') or ''))
        for key in ('WLAN radios', 'SDR receivers'):
            radios = status_json.get(key)
            if isinstance(radios, dict):
                for name, state in radios.items():
                    level = (st.StatusReport.STATUS_LEVEL_INFORMATION_STATUS if state == 'ok'
                             else st.StatusReport.STATUS_LEVEL_WARNING_STATUS)
                    r.status.add(status_level=level, status_type=st.StatusReport.STATUS_TYPE_OTHER,
                                 status_value='%s %s: %s' % (key, name, state))
    return m


# --- detections ---------------------------------------------------------------------

def _enu(speed, direction, vertical):
    """ENU velocity from ground speed (m/s), track (degrees true) and vertical rate (m/s)."""
    if not _known(speed) or not _known(direction):
        return None
    if speed == 0 and direction == 0 and not (_known(vertical) and vertical != 0):
        return None              # 0/0 before the first velocity message: unknown
    v = vel.ENUVelocity(east_rate=speed * math.sin(math.radians(direction)),
                        north_rate=speed * math.cos(math.radians(direction)))
    if _known(vertical):
        v.up_rate = float(vertical)
    return v


def _classification(report, cls_sub, confidence=1.0):
    cls, sub = cls_sub
    c = report.classification.add(type=cls, confidence=confidence)
    if sub:
        c.sub_class.add(type=sub, confidence=confidence, level=1)


def _info(report, ftype, value):
    if value is None or value == '' or (isinstance(value, float) and math.isnan(value)):
        return
    if isinstance(value, float):
        value = ('%.7f' % value).rstrip('0').rstrip('.')
    report.object_info.add(type=ftype, value=str(value))


def wlan_channel_mhz(channel):
    """WLAN channel number -> centre frequency in MHz, None if unknown (0 = Bluetooth).
    Frequencies are sent in MHz: the SAPIENT fields are float32, too coarse for Hz at 2.4 GHz."""
    try:
        ch = int(channel)
    except (TypeError, ValueError):
        return None
    if 1 <= ch <= 13:
        return 2407.0 + 5 * ch
    if ch == 14:
        return 2484.0
    if 32 <= ch <= 177:
        return 5000.0 + 5 * ch
    return None


def remote_id_detection(node_id, object_id, msg, state=None, settings=None, now=None):
    """DetectionReport for a RemoteIDMessage, None without a drone position."""
    s = _settings(settings)
    uas = msg.uas
    lo = uas.location
    if not lo.has_position:
        return None
    m = envelope(node_id, now)
    r = m.detection_report
    r.report_id = new_ulid(now)
    r.object_id = object_id
    if state:
        r.state = state
    r.location.CopyFrom(_location(lo.latitude, lo.longitude, lo.altitude_geo, WGS84_E,
                                  _HORIZ_ACCURACY_M.get(lo.horiz_accuracy),
                                  _HORIZ_ACCURACY_M.get(lo.horiz_accuracy),
                                  _VERT_ACCURACY_M.get(lo.vert_accuracy)))
    r.detection_confidence = 1.0          # decoded broadcast, not a guess
    v = _enu(lo.speed_horizontal, lo.direction, lo.speed_vertical)
    if v is not None:
        r.enu_velocity.CopyFrom(v)
    _classification(r, s['class_drone'])
    r.id = decode.drone_id(msg)

    _info(r, 'source', 'RemoteID %s' % (msg.type or ''))
    _info(r, 'serial number', uas.serial_number)
    for b in uas.basic_id:
        if b.valid and b.id_type not in (0, 1):
            _info(r, 'UAS ID', b.id_text if b.is_text else b.id_raw.rstrip(b'\x00').hex())
            _info(r, 'UAS ID type', open_drone_id.decode_basicID_ID_type(b.id_type))
    if uas.basic_id[0].valid:
        _info(r, 'UA type', open_drone_id.decode_basicID_UA_type(uas.basic_id[0].ua_type))
    if lo.valid:
        _info(r, 'status', open_drone_id.decode_location_status(lo.status))
        _info(r, 'height', lo.height)
        if _known(lo.height):
            _info(r, 'height type', 'above ground' if lo.height_type == 1 else 'above takeoff')
    if uas.operator_id.valid:
        _info(r, 'operator ID', uas.operator_id.id_text)
    sys_ = uas.system
    if sys_.has_operator_position:
        _info(r, 'operator latitude', sys_.operator_latitude)
        _info(r, 'operator longitude', sys_.operator_longitude)
        _info(r, 'operator altitude', sys_.operator_altitude_geo)
        _info(r, 'operator location type',
              open_drone_id.decode_system_operator_location_type(sys_.operator_location_type))
    if sys_.valid and sys_.classification_type == 1:
        _info(r, 'EU category', open_drone_id.decode_system_ua_category(sys_.category_eu))
        _info(r, 'EU class', open_drone_id.decode_system_ua_class(sys_.class_eu))
    if uas.self_id.valid:
        _info(r, 'self-ID', uas.self_id.desc_raw.split(b'\x00')[0].decode('ascii', 'replace').strip())
    _info(r, 'MAC address', msg.mac_address)
    if msg.extra.get('SN present') == 1:
        _info(r, 'manufacturer', msg.extra.get('manufacturer'))
        _info(r, 'model', msg.extra.get('model'))

    if _known(msg.rssi):
        sig = r.signal.add(amplitude=float(msg.rssi))
        mhz = wlan_channel_mhz(msg.channel)
        if mhz:
            sig.centre_frequency = mhz
        else:                                     # Bluetooth: somewhere in the 2.4 GHz band
            sig.centre_frequency, sig.start_frequency, sig.stop_frequency = 2441.75, 2400.0, 2483.5
    return m


def aircraft_class(msg, settings=None):
    """(class, sub-class) from the emitter category (ADS-B / UAT categories)."""
    s = _settings(settings)
    try:
        category = int(msg.get('category'))
    except (TypeError, ValueError):
        return s['class_aircraft']
    if category in (1, 2, 3, 4, 5, 6, 9, 12):   # weight classes, highly maneuverable, glider, ultralight
        return s['class_fixed_wing']
    if category == 7:
        return s['class_rotorcraft']
    if category == 10:
        return s['class_lighter_than_air']
    if category == 14:                          # UAV with ADS-B
        return s['class_drone']
    return s['class_aircraft']


def aircraft_key(msg):
    """Object key of an aircraft: ICAO address for ADS-B/UAT, else technology + address."""
    address = (msg.get('address') or '').upper()
    if msg.get('type') in ('ADSB', 'UAT') and msg.get('address type') in (0, 2):
        return 'icao-' + address
    return '%s-%s' % ('-'.join((msg.get('type') or 'aircraft').lower().split()), address)


def aircraft_detection(node_id, object_id, msg, state=None, settings=None, now=None):
    """DetectionReport for an AircraftMessage, None without a position."""
    s = _settings(settings)
    try:
        lat, lon = float(msg.get('latitude')), float(msg.get('longitude'))
    except (TypeError, ValueError):
        return None
    if lat == 0.0 and lon == 0.0:
        return None
    alt_type = msg.get('altitude type')
    datum = WGS84_E if alt_type == 2 else WGS84_G     # 2 GNSS height; 1 baro and 3 GNSS MSL: ~geoid
    m = envelope(node_id, now)
    r = m.detection_report
    r.report_id = new_ulid(now)
    r.object_id = object_id
    if state:
        r.state = state
    r.location.CopyFrom(_location(lat, lon, msg.get('altitude'), datum))
    r.detection_confidence = 1.0
    v = _enu(msg.get('speed'), msg.get('track'), msg.get('vertical rate'))
    if v is not None:
        r.enu_velocity.CopyFrom(v)
    _classification(r, aircraft_class(msg, s))
    flight = (msg.get('flight') or '').strip()
    address = (msg.get('address') or '').upper()
    r.id = flight or address

    _info(r, 'source', msg.get('type'))
    _info(r, 'address', address)
    if msg.get('type') in ('ADSB', 'UAT') and msg.get('address type') in (0, 2):
        _info(r, 'ICAO address', address)
    if msg.get('address type') is not None:
        _info(r, 'address type', aircraft_mod.decode_address_type(msg.get('address type')))
    _info(r, 'callsign', flight)
    if msg.get('category') is not None:
        _info(r, 'emitter category', aircraft_mod.decode_emitter_category(msg.get('category'), msg.get('type')))
    if alt_type is not None:
        _info(r, 'altitude type', aircraft_mod.decode_altitude_type(alt_type))
    if msg.get('flight state') is not None:
        _info(r, 'flight state', aircraft_mod.decode_flight_state(msg.get('flight state')))
    if msg.get('emergency status') is not None:
        _info(r, 'emergency', 'true' if msg.get('emergency status') else 'false')

    if _known(msg.get('RSSI')):
        sig = r.signal.add(amplitude=float(msg.get('RSSI')))
        try:
            sig.centre_frequency = float(msg.get('frequency MHz'))
        except (TypeError, ValueError):
            sig.centre_frequency = 0.0
    return m


def lost_detection(node_id, object_id, last, now=None):
    """Final report of a track that wasn't seen for a while: the last report again with
    a new report_id and state "lost" (the ICD asks for a final detection for lost tracks)."""
    m = envelope(node_id, now)
    m.detection_report.CopyFrom(last.detection_report)
    m.detection_report.report_id = new_ulid(now)
    m.detection_report.object_id = object_id
    m.detection_report.state = 'lost'
    return m


# --- tasking ----------------------------------------------------------------------------

def task_ack(node_id, task_id, reason='tasking is not supported by this node', now=None):
    m = envelope(node_id, now)
    m.task_ack.task_id = task_id
    m.task_ack.task_status = tack.TaskAck.TASK_STATUS_REJECTED
    m.task_ack.reason.append(reason)
    return m


# --- framing ------------------------------------------------------------------------------

def frame(message):
    """SAPIENT over TCP: 4-byte little-endian length, then the serialized SapientMessage."""
    data = message.SerializeToString()
    return len(data).to_bytes(4, 'little') + data


def parse(data):
    m = sm.SapientMessage()
    m.ParseFromString(data)
    return m

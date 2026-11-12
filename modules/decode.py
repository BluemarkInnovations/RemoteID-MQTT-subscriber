# (c) Bluemark Innovations BV
# MIT license
"""Decode DroneScout MQTT messages once into plain Python objects.

Console printing, CSV logging and the exporters (SBS, and later CoT / SAPIENT /
ASTERIX) all work from these objects instead of reading the UASdata byte
offsets themselves.

The sanity checks (out-of-range values -> NaN) are the ones the subscriber has
always used; an exporter that needs another "unknown" value (e.g. 0.0 for SBS)
converts NaN itself.
"""

import base64
import math
import struct
from dataclasses import dataclass, field
from typing import List, Optional

# offsets in the UASdata struct (ODID_UAS_Data as sent by the sensor)
BASIC_ID_OFFSETS = (0, 32)
LOCATION_OFFSET = 64
AUTH_OFFSET = 136
AUTH_PAGE_SIZE = 40
AUTH_PAGES = 14          # pages 0..13 are in the payload (valid flags 895..908)
SELFID_OFFSET = 776
SYSTEM_OFFSET = 808
OPERATORID_OFFSET = 864
VALID_OFFSET = 892
UASDATA_MIN_LENGTH = 914

GPS_EPOCH = 1546300800   # 2019-01-01 00:00:00 UTC, base of the System / Auth timestamps

NAN = float("NaN")


# One precompiled layout per block ('=': native byte order like the sensor, no padding).
_BASIC_ID = struct.Struct('=II21s')                 # ua_type, id_type, id
_LOCATION = struct.Struct('=IfffddffIfIIIIIf')      # status .. timestamp (72 bytes)
_AUTH_PAGE = struct.Struct('=B3xB3xBBxxI23s')       # data_page, auth_type, last_page_index, length, timestamp, data
_SELF_ID = struct.Struct('=I23s')
_SYSTEM = struct.Struct('=IIddHHffIIfI')
_OPERATOR_ID = struct.Struct('=I20s')


def nan_to(value, default):
    """NaN (unknown) -> default, for outputs that have no 'unknown' value."""
    return default if isinstance(value, float) and math.isnan(value) else value


@dataclass
class BasicID:
    valid: bool
    ua_type: int
    id_type: int
    id_raw: bytes            # 21 bytes, as sent

    @property
    def is_text(self):
        """Serial number and CAA registration ID are ASCII, the other ID types binary."""
        return self.id_type in (1, 2)

    @property
    def id_text(self):
        """ASCII ID up to the first NUL (serial / registration), '' if not ASCII."""
        try:
            return self.id_raw.decode('ascii').split('\x00')[0]
        except UnicodeDecodeError:
            return ''


@dataclass
class Location:
    valid: bool
    status: int              # 0 undeclared, 1 ground, 2 airborne, 3 emergency, 4 RID failure
    direction: float         # degrees, NaN = unknown
    speed_horizontal: float  # m/s, NaN = unknown
    speed_vertical: float    # m/s, NaN = unknown
    latitude: float          # NaN = unknown
    longitude: float
    altitude_baro: float     # m, NaN = unknown
    altitude_geo: float      # m WGS84, NaN = unknown
    height_type: int         # 0 above takeoff, 1 AGL
    height: float            # m, NaN = unknown
    horiz_accuracy: int
    vert_accuracy: int
    baro_accuracy: int
    speed_accuracy: int
    ts_accuracy: int
    timestamp: float         # seconds after the full hour

    @property
    def has_position(self):
        return self.valid and not math.isnan(self.latitude) and not math.isnan(self.longitude)


@dataclass
class AuthPage:
    valid: bool
    data_page: int
    auth_type: int
    last_page_index: int     # only meaningful in page 0
    length: int              # only meaningful in page 0
    timestamp: int           # only meaningful in page 0
    data_raw: bytes          # 23 bytes from offset 16 (page 0 uses the first 17)


@dataclass
class SelfID:
    valid: bool
    desc_type: int
    desc_raw: bytes          # 23 bytes


@dataclass
class System:
    valid: bool
    operator_location_type: int
    classification_type: int
    operator_latitude: float   # NaN = unknown
    operator_longitude: float
    area_count: int
    area_radius: int
    area_ceiling: float
    area_floor: float
    category_eu: int
    class_eu: int
    operator_altitude_geo: float
    timestamp: int             # seconds since 2019-01-01, 0 = invalid

    @property
    def has_operator_position(self):
        return (self.valid and not math.isnan(self.operator_latitude)
                and not math.isnan(self.operator_longitude))


@dataclass
class OperatorID:
    valid: bool
    id_type: int
    id_raw: bytes            # 20 bytes

    @property
    def id_text(self):
        try:
            return self.id_raw.decode('ascii').split('\x00')[0]
        except UnicodeDecodeError:
            return ''


class UASData:
    """Decoded UASdata block of one RemoteID data message.

    The auth pages are only decoded when something reads .auth (CSV, console);
    most outputs never need them."""

    __slots__ = ('raw', 'basic_id', 'location', 'self_id', 'system', 'operator_id', '_auth')

    def __init__(self, raw, basic_id, location, self_id, system, operator_id, auth=None):
        self.raw = raw
        self.basic_id = basic_id
        self.location = location
        self.self_id = self_id
        self.system = system
        self.operator_id = operator_id
        self._auth = auth

    @property
    def auth(self):
        if self._auth is None:
            self._auth = _decode_auth(self.raw)
        return self._auth

    @property
    def serial_number(self):
        """First valid serial number (ID type 1), else ''."""
        for b in self.basic_id:
            if b.valid and b.id_type == 1:
                return b.id_text
        return ''

    def __repr__(self):
        return ("UASData(basic_id=%r, location=%r, self_id=%r, system=%r, operator_id=%r)"
                % (self.basic_id, self.location, self.self_id, self.system, self.operator_id))


@dataclass
class RemoteIDMessage:
    """One "data" message from a sensor."""
    sensor_id: Optional[str]
    timestamp_ms: Optional[int]
    rssi: Optional[float]
    channel: Optional[int]
    mac_address: Optional[str]
    type: Optional[str]          # e.g. "WLAN Beacon", "BT5 long range"
    raw: Optional[bytes]         # received frame, None if not sent
    extra: dict                  # serial number lookup (manufacturer, model, ...)
    uas: UASData
    json: dict = field(repr=False, default_factory=dict)   # the original "data" object


@dataclass
class AircraftMessage:
    """One "aircraft" message (ADS-B 1090, UAT 978, ADS-L / FLARM / OGN)."""
    json: dict = field(repr=False)
    raw: bytes                   # received frame, trailing NUL / newline removed
    sbs_lines: List[str]         # SBS BaseStation lines generated by the sensor

    def get(self, key, default=None):
        return self.json.get(key, default)

    @property
    def sensor_id(self):
        return self.json.get('sensor ID')

    @property
    def address(self):
        return self.json.get('address')


def _decode_auth(p):
    v = p[VALID_OFFSET + 3:VALID_OFFSET + 3 + AUTH_PAGES]
    unpack = _AUTH_PAGE.unpack_from
    pages = []
    for page in range(AUTH_PAGES):
        data_page, auth_type, last_page_index, length, timestamp, data = unpack(p, AUTH_OFFSET + AUTH_PAGE_SIZE * page)
        pages.append(AuthPage(v[page] == 1, data_page, auth_type, last_page_index, length, timestamp, data))
    return pages


def decode_uas_data(payload):
    """UASdata bytes -> UASData. Raises ValueError when the payload is too short."""
    p = payload if isinstance(payload, bytes) else bytes(payload)
    if len(p) < UASDATA_MIN_LENGTH:
        raise ValueError("UASdata too short: %d bytes" % len(p))
    v = p[VALID_OFFSET:VALID_OFFSET + 22]

    ua_type, id_type, id_raw = _BASIC_ID.unpack_from(p, 0)
    basic_id0 = BasicID(v[0] == 1, ua_type, id_type, id_raw)
    ua_type, id_type, id_raw = _BASIC_ID.unpack_from(p, 32)
    basic_id1 = BasicID(v[1] == 1, ua_type, id_type, id_raw)

    (status, direction, speed_h, speed_v, lat, lon, alt_baro, alt_geo, height_type, height,
     horiz_acc, vert_acc, baro_acc, speed_acc, ts_acc, ts) = _LOCATION.unpack_from(p, LOCATION_OFFSET)
    location = Location(
        v[2] == 1, status,
        NAN if direction > 360 or direction < 0 else direction,
        NAN if speed_h > 254.25 or speed_h < 0 else speed_h,
        NAN if speed_v > 62 or speed_v < -62 else speed_v,
        NAN if lat == 0.0 or lat > 90.0 or lat < -90.0 else lat,
        NAN if lon == 0.0 or lon > 180.0 or lon < -180.0 else lon,
        NAN if alt_baro <= -1000.0 or alt_baro > 31767.5 else alt_baro,
        NAN if alt_geo <= -1000.0 or alt_geo > 31767.5 else alt_geo,
        height_type,
        NAN if height <= -1000.0 or height > 31767.5 else height,
        horiz_acc, vert_acc, baro_acc, speed_acc, ts_acc, ts)

    desc_type, desc_raw = _SELF_ID.unpack_from(p, SELFID_OFFSET)
    self_id = SelfID(v[19] == 1, desc_type, desc_raw)

    (op_loc_type, class_type, op_lat, op_lon, area_count, area_radius, area_ceiling, area_floor,
     category_eu, class_eu, op_alt, sys_ts) = _SYSTEM.unpack_from(p, SYSTEM_OFFSET)
    system = System(
        v[20] == 1, op_loc_type, class_type,
        NAN if op_lat == 0.0 or op_lat > 90.0 or op_lat < -90.0 else op_lat,
        NAN if op_lon == 0.0 or op_lon > 180.0 or op_lon < -180.0 else op_lon,
        area_count, area_radius,
        NAN if area_ceiling == -1000 else area_ceiling,
        NAN if area_floor == -1000 else area_floor,
        category_eu, class_eu,
        NAN if op_alt <= -1000.0 or op_alt > 31767.5 else op_alt,
        sys_ts)

    op_id_type, op_id_raw = _OPERATOR_ID.unpack_from(p, OPERATORID_OFFSET)
    operator_id = OperatorID(v[21] == 1, op_id_type, op_id_raw)

    return UASData(p, [basic_id0, basic_id1], location, self_id, system, operator_id)


def decode_remote_id(data_json):
    """The "data" object of a RemoteID message -> RemoteIDMessage."""
    uas = decode_uas_data(base64.b64decode(data_json.get('UASdata')))
    try:
        raw = base64.b64decode(data_json.get('raw'))
    except Exception:
        raw = None
    extra = data_json.get('extra')
    if not isinstance(extra, dict):
        extra = {}
    return RemoteIDMessage(
        sensor_id=data_json.get('sensor ID'),
        timestamp_ms=data_json.get('timestamp'),
        rssi=data_json.get('RSSI'),
        channel=data_json.get('channel'),
        mac_address=data_json.get('MAC address'),
        type=data_json.get('type'),
        raw=raw,
        extra=extra,
        uas=uas,
        json=data_json,
    )


def _strip_terminator(value):
    """Remove one trailing NUL or newline, as the sensor sometimes sends one."""
    if len(value) > 0 and value[-1:] in (b'\x00', b'\n', '\x00', '\n'):
        return value[:-1]
    return value


def decode_aircraft(aircraft_json):
    """The "aircraft" object -> AircraftMessage."""
    try:
        raw = _strip_terminator(base64.b64decode(aircraft_json.get('raw')))
    except Exception:
        raw = b''
    sbs = aircraft_json.get('SBS')
    sbs_lines = _strip_terminator(sbs).split(';') if isinstance(sbs, str) and sbs else []
    return AircraftMessage(json=aircraft_json, raw=raw, sbs_lines=sbs_lines)


def drone_id(msg):
    """Stable ID of a drone (RemoteIDMessage): serial number, else another valid
    basic ID (ASCII, or hex for binary IDs), else the MAC address without colons.
    Used as the object key by the TAK, SAPIENT and ASTERIX outputs."""
    uas = msg.uas
    if uas.serial_number:
        return uas.serial_number
    for b in uas.basic_id:
        if b.valid and b.id_type != 0:
            text = b.id_text if b.is_text else b.id_raw.rstrip(b'\x00').hex()
            if text:
                return text
    return (msg.mac_address or '').replace(':', '')

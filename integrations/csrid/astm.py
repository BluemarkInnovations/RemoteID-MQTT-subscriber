# (c) Bluemark Innovations BV
# MIT license
"""ASTM F3411-22a Broadcast RemoteID messages (25 bytes each, protocol version 2) from the
decoded UASdata of a DroneScout RemoteID message.

The sensor sends the decoded ODID_UAS_Data, not always the received frame, so the Finder
re-encodes the B-RID messages that the drone broadcast: Basic ID (up to two),
Location, Authentication pages, Self ID, System and Operator ID, in a Message Pack.
Encoding as in the opendroneid-core-c reference implementation (encodeDirection,
encodeSpeedHorizontal, encodeAltitude, ...).
"""

import math
import struct

PROTOCOL_VERSION = 2
MSG_SIZE = 25
PACK_MAX = 9                    # messages in one Message Pack

BASIC_ID, LOCATION, AUTH, SELF_ID, SYSTEM, OPERATOR_ID, MESSAGE_PACK = 0, 1, 2, 3, 4, 5, 0xF


def _header(msg_type):
    return bytes([(msg_type << 4) | PROTOCOL_VERSION])


def _bad(x):
    return x is None or (isinstance(x, float) and math.isnan(x))


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


def _latlon(v):
    return 0 if _bad(v) else int(round(v * 1e7))


def _altitude(v):
    """m -> uint16 in 0.5 m steps with -1000 m offset; 0 = unknown."""
    if _bad(v) or v <= -1000:
        return 0
    return _clamp(int(round((v + 1000) / 0.5)), 0, 0xFFFF)


def _direction(d):
    """degrees -> (E/W bit, 0..179 byte); unknown = 361."""
    if _bad(d) or d < 0 or d > 360:
        d = 361
    d = int(round(d))
    if d < 180:
        return 0, d
    return 1, d - 180


def _speed_horizontal(s):
    """m/s -> (multiplier bit, byte); unknown = 255 with multiplier 1."""
    if _bad(s) or s < 0:
        return 1, 255
    if s <= 255 * 0.25:
        return 0, int(round(s / 0.25))
    if s < 254.25:
        return 1, int(round((s - 255 * 0.25) / 0.75))
    return 1, 254


def _speed_vertical(v):
    """m/s -> int8 in 0.5 m/s steps; unknown = 63 m/s."""
    if _bad(v):
        v = 63.0
    return _clamp(int(round(v / 0.5)), -124, 126)


def _text(raw, size):
    return bytes(raw[:size]).ljust(size, b'\x00')


def basic_id(b):
    return _header(BASIC_ID) + bytes([(b.id_type << 4) | (b.ua_type & 0x0f)]) + _text(b.id_raw, 20) + bytes(3)


def location(loc):
    ew, direction = _direction(loc.direction)
    mult, speed = _speed_horizontal(loc.speed_horizontal)
    flags = (loc.status << 4) | ((loc.height_type & 1) << 2) | (ew << 1) | mult
    ts = 0xFFFF if _bad(loc.timestamp) or loc.timestamp < 0 else _clamp(int(round(loc.timestamp * 10)), 0, 36000)
    return _header(LOCATION) + struct.pack(
        '<BBBbiiHHHBBHBx', flags, direction, speed, _speed_vertical(loc.speed_vertical),
        _latlon(loc.latitude), _latlon(loc.longitude), _altitude(loc.altitude_baro),
        _altitude(loc.altitude_geo), _altitude(loc.height),
        ((loc.vert_accuracy & 0x0f) << 4) | (loc.horiz_accuracy & 0x0f),
        ((loc.baro_accuracy & 0x0f) << 4) | (loc.speed_accuracy & 0x0f), ts, loc.ts_accuracy & 0x0f)


def auth(page):
    head = _header(AUTH) + bytes([((page.auth_type & 0x0f) << 4) | (page.data_page & 0x0f)])
    if page.data_page == 0:
        return head + struct.pack('<BBI', page.last_page_index, page.length, page.timestamp) + _text(page.data_raw, 17)
    return head + _text(page.data_raw, 23)


def self_id(s):
    return _header(SELF_ID) + bytes([s.desc_type & 0xff]) + _text(s.desc_raw, 23)


def system(s):
    flags = ((s.classification_type & 0x07) << 2) | (s.operator_location_type & 0x03)
    radius = _clamp(int(round((s.area_radius or 0) / 10)), 0, 255)
    return _header(SYSTEM) + struct.pack(
        '<BiiHBHHBHIx', flags, _latlon(s.operator_latitude), _latlon(s.operator_longitude),
        _clamp(s.area_count or 0, 0, 0xFFFF), radius, _altitude(s.area_ceiling), _altitude(s.area_floor),
        ((s.category_eu & 0x0f) << 4) | (s.class_eu & 0x0f), _altitude(s.operator_altitude_geo),
        s.timestamp & 0xFFFFFFFF)


def operator_id(o):
    return _header(OPERATOR_ID) + bytes([o.id_type & 0xff]) + _text(o.id_raw, 20) + bytes(3)


def messages(uas):
    """The valid B-RID messages of a decode.UASData, 25 bytes each."""
    out = [basic_id(b) for b in uas.basic_id if b.valid]
    if uas.location.valid:
        out.append(location(uas.location))
    out += [auth(p) for p in (uas.auth or []) if p.valid]
    if uas.self_id.valid:
        out.append(self_id(uas.self_id))
    if uas.system.valid:
        out.append(system(uas.system))
    if uas.operator_id.valid:
        out.append(operator_id(uas.operator_id))
    for m in out:
        assert len(m) == MSG_SIZE, len(m)
    return out


def message_pack(msgs):
    if not 0 < len(msgs) <= PACK_MAX:
        raise ValueError("a Message Pack holds 1 to %d messages" % PACK_MAX)
    return _header(MESSAGE_PACK) + bytes([MSG_SIZE, len(msgs)]) + b''.join(msgs)


def message_packs(uas):
    """The messages in Message Packs of at most 9 (more than one only with many auth pages)."""
    msgs = messages(uas)
    return [message_pack(msgs[i:i + PACK_MAX]) for i in range(0, len(msgs), PACK_MAX)]

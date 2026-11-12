# (c) Bluemark Innovations BV
# MIT license
"""ASTM F3411-22a Broadcast RemoteID decoder for the reference SDSP: B-RID messages and Message
Packs -> dicts. Written from the standard, independent of the Finder's encoder (astm.py), so
the tests check one against the other. Standard library only.
"""

import datetime
import struct


def _alt(raw):
    return None if raw == 0 else raw * 0.5 - 1000


def _ascii(b):
    return b.split(b'\x00')[0].decode('ascii', 'replace')


def decode_brid(data):
    """A B-RID message or Message Pack -> list of dicts."""
    if len(data) < 1:
        return []
    mtype, version = data[0] >> 4, data[0] & 0x0f
    if mtype == 0xF:
        if len(data) < 3:
            raise ValueError("Message Pack too short")
        size, count = data[1], data[2]
        if size != 25 or len(data) < 3 + size * count:
            raise ValueError("bad Message Pack: size %d, %d messages, %d bytes" % (size, count, len(data)))
        out = []
        for i in range(count):
            out += decode_brid(data[3 + 25 * i:3 + 25 * (i + 1)])
        return out
    if len(data) < 25:
        raise ValueError("B-RID message of %d bytes" % len(data))
    m = data[:25]
    d = {'version': version}
    if mtype == 0:
        id_type, ua_type = m[1] >> 4, m[1] & 0x0f
        raw = m[2:22]
        d.update(type='basic_id', id_type=id_type, ua_type=ua_type,
                 uas_id=_ascii(raw) if id_type in (1, 2) else raw.rstrip(b'\x00').hex())
    elif mtype == 1:
        flags, direction, raw_speed, vspeed, lat, lon, alt_baro, alt_geo, height, acc1, acc2, ts, ts_acc = \
            struct.unpack('<BBBbiiHHHBBHB', m[1:24])
        mult, ew = flags & 1, (flags >> 1) & 1
        direction = direction + 180 * ew
        speed = raw_speed * 0.25 if mult == 0 else raw_speed * 0.75 + 63.75
        d.update(type='location', status=flags >> 4, height_type=(flags >> 2) & 1,
                 direction=None if direction > 360 else direction,
                 speed=None if mult == 1 and raw_speed == 255 else speed,
                 vertical_speed=None if vspeed * 0.5 == 63 else vspeed * 0.5,
                 latitude=lat / 1e7 if lat else None, longitude=lon / 1e7 if lon else None,
                 altitude_baro=_alt(alt_baro), altitude_geo=_alt(alt_geo), height=_alt(height),
                 horizontal_accuracy=acc1 & 0x0f, vertical_accuracy=acc1 >> 4,
                 timestamp=None if ts == 0xFFFF else ts / 10.0)
    elif mtype == 2:
        d.update(type='auth', auth_type=m[1] >> 4, page=m[1] & 0x0f)
    elif mtype == 3:
        d.update(type='self_id', desc_type=m[1], text=_ascii(m[2:25]))
    elif mtype == 4:
        flags, op_lat, op_lon, area_count, radius, ceiling, floor, cat_class, op_alt, ts = \
            struct.unpack('<BiiHBHHBHI', m[1:24])
        d.update(type='system', operator_location_type=flags & 3, classification_type=(flags >> 2) & 7,
                 operator_latitude=op_lat / 1e7 if op_lat else None,
                 operator_longitude=op_lon / 1e7 if op_lon else None, area_count=area_count,
                 area_radius=radius * 10, category_eu=cat_class >> 4, class_eu=cat_class & 0x0f,
                 operator_altitude=_alt(op_alt),
                 timestamp=datetime.datetime.fromtimestamp(1546300800 + ts, datetime.timezone.utc).isoformat()
                 if ts else None)
    elif mtype == 5:
        d.update(type='operator_id', id_type=m[1], operator_id=_ascii(m[2:22]))
    else:
        d.update(type='unknown %d' % mtype)
    return [d]

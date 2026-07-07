"""
Shared fixtures for the test suite.

The opendroneid payload layout (byte offsets, struct format codes) is
hard-coded in modules/open_drone_id.py and modules/log_remote_id.py. This
module rebuilds that same layout from scratch so tests can construct
synthetic, deterministic payloads instead of relying on captured traffic.
"""
import struct
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mqtt_sub import open_drone_id_valid_blocks
from modules import open_drone_id

PAYLOAD_LENGTH = 914

BASIC_ID0_OFFSET = 0
BASIC_ID1_OFFSET = 32
LOCATION_OFFSET = 64
AUTH_BASE_OFFSET = 136
AUTH_PAGE_SIZE = 40
SELFID_OFFSET = 776
SYSTEM_OFFSET = 808
OPERATORID_OFFSET = 864
VALID_FLAGS_OFFSET = 892

GPS_EPOCH = 1546300800  # 2019-01-01T00:00:00Z, used by System/Auth timestamps


def _fixed_width_ascii(value, width):
    data = value.encode('ascii')
    assert len(data) == width, f"{value!r} must be exactly {width} bytes"
    return data


class PayloadBuilder:
    """Builds a synthetic opendroneid UASdata payload byte-by-byte."""

    def __init__(self):
        self.buf = bytearray(PAYLOAD_LENGTH)

    def basic_id(self, page, ua_type, id_type, basic_id_str):
        offset = BASIC_ID0_OFFSET if page == 0 else BASIC_ID1_OFFSET
        struct.pack_into('I', self.buf, offset, ua_type)
        struct.pack_into('I', self.buf, offset + 4, id_type)
        self.buf[offset + 8:offset + 8 + 21] = _fixed_width_ascii(basic_id_str, 21)
        return self

    def location(self, status=0, direction=0.0, speed_horizontal=0.0,
                 speed_vertical=0.0, latitude=0.0, longitude=0.0,
                 altitude_baro=0.0, altitude_geo=0.0, height_type=0,
                 height=0.0, horiz_accuracy=0, vert_accuracy=0,
                 baro_accuracy=0, speed_accuracy=0, ts_accuracy=0,
                 timestamp=0.0):
        o = LOCATION_OFFSET
        struct.pack_into('I', self.buf, o, status)
        struct.pack_into('f', self.buf, o + 4, direction)
        struct.pack_into('f', self.buf, o + 8, speed_horizontal)
        struct.pack_into('f', self.buf, o + 12, speed_vertical)
        struct.pack_into('d', self.buf, o + 16, latitude)
        struct.pack_into('d', self.buf, o + 24, longitude)
        struct.pack_into('f', self.buf, o + 32, altitude_baro)
        struct.pack_into('f', self.buf, o + 36, altitude_geo)
        struct.pack_into('I', self.buf, o + 40, height_type)
        struct.pack_into('f', self.buf, o + 44, height)
        struct.pack_into('I', self.buf, o + 48, horiz_accuracy)
        struct.pack_into('I', self.buf, o + 52, vert_accuracy)
        struct.pack_into('I', self.buf, o + 56, baro_accuracy)
        struct.pack_into('I', self.buf, o + 60, speed_accuracy)
        struct.pack_into('I', self.buf, o + 64, ts_accuracy)
        struct.pack_into('f', self.buf, o + 68, timestamp)
        return self

    def self_id(self, desc_type=0, desc=""):
        o = SELFID_OFFSET
        struct.pack_into('I', self.buf, o, desc_type)
        self.buf[o + 4:o + 4 + 23] = _fixed_width_ascii(desc, 23)
        return self

    def system(self, operator_location_type=0, classification_type=0,
               operator_latitude=0.0, operator_longitude=0.0, area_count=0,
               area_radius=0, area_ceiling=0.0, area_floor=0.0,
               category_eu=0, class_eu=0, operator_altitude_geo=0.0,
               timestamp=0):
        o = SYSTEM_OFFSET
        struct.pack_into('I', self.buf, o, operator_location_type)
        struct.pack_into('I', self.buf, o + 4, classification_type)
        struct.pack_into('d', self.buf, o + 8, operator_latitude)
        struct.pack_into('d', self.buf, o + 16, operator_longitude)
        struct.pack_into('H', self.buf, o + 24, area_count)
        struct.pack_into('H', self.buf, o + 26, area_radius)
        struct.pack_into('f', self.buf, o + 28, area_ceiling)
        struct.pack_into('f', self.buf, o + 32, area_floor)
        struct.pack_into('I', self.buf, o + 36, category_eu)
        struct.pack_into('I', self.buf, o + 40, class_eu)
        struct.pack_into('f', self.buf, o + 44, operator_altitude_geo)
        struct.pack_into('I', self.buf, o + 48, timestamp)
        return self

    def operator_id(self, operator_id_type=0, operator_id=""):
        o = OPERATORID_OFFSET
        struct.pack_into('I', self.buf, o, operator_id_type)
        self.buf[o + 4:o + 4 + 20] = _fixed_width_ascii(operator_id, 20)
        return self

    def auth_page(self, page, auth_type=0, data=b"", last_page_index=0,
                  length=0, timestamp=0):
        o = AUTH_BASE_OFFSET + AUTH_PAGE_SIZE * page
        struct.pack_into('B', self.buf, o, page)
        struct.pack_into('B', self.buf, o + 4, auth_type)
        if page == 0:
            struct.pack_into('B', self.buf, o + 8, last_page_index)
            struct.pack_into('B', self.buf, o + 9, length)
            struct.pack_into('I', self.buf, o + 12, timestamp)
        self.buf[o + 16:o + 16 + len(data)] = data
        return self

    def valid_flags(self, basic_id0=0, basic_id1=0, location=0,
                     auth=None, self_id=0, system=0, operator_id=0):
        o = VALID_FLAGS_OFFSET
        auth = auth or [0] * 14
        assert len(auth) == 14
        self.buf[o + 0] = basic_id0
        self.buf[o + 1] = basic_id1
        self.buf[o + 2] = location
        for i, v in enumerate(auth):
            self.buf[o + 3 + i] = v
        self.buf[o + 19] = self_id
        self.buf[o + 20] = system
        self.buf[o + 21] = operator_id
        return self

    def build(self):
        assert len(self.buf) == PAYLOAD_LENGTH
        return bytes(self.buf)


@pytest.fixture
def payload_builder():
    return PayloadBuilder()


# Fixed-width strings generated (not hand-counted) to guarantee exact length.
BASIC_ID0_STR = ("BASICID0-" + "A" * 30)[:21]
BASIC_ID1_STR = ("BASICID1-" + "B" * 30)[:21]
SELFID_DESC = ("Flying over the park" + " " * 30)[:23]
OPERATOR_ID_STR = ("OPID-" + "C" * 30)[:20]
AUTH_PAGE0_DATA = bytes(range(17))
AUTH_PAGE1_DATA = bytes(range(5))


class Golden:
    """A fully populated, internally-consistent RemoteID message.

    UAType_1 is deliberately NOT in {1, 2} while IDType_1 IS, so that any
    code path that mistakenly branches on UAType_1 instead of IDType_1 (see
    the historical bug in log_remote_id.py) is caught by tests.
    """

    ua_type_0 = 2       # Helicopter (or Multirotor)
    id_type_0 = 1        # Serial Number
    ua_type_1 = 3         # Gyroplane (NOT 1 or 2)
    id_type_1 = 2          # CAA Registration ID (IS 1 or 2 -> ascii)

    status = 2  # Airborne
    direction = 45.0
    speed_horizontal = 12.5
    speed_vertical = -2.0
    latitude = 52.379189
    longitude = 4.899431
    altitude_baro = 100.5
    altitude_geo = 105.25
    height_type = 1  # Above Ground Level
    height = 50.0
    horiz_accuracy = 10  # 10 meter
    vert_accuracy = 4    # 10 meter
    baro_accuracy = 4    # 10 meter
    speed_accuracy = 2   # 3 m/s
    ts_accuracy = 5      # 0.5 s
    location_timestamp = 30.5  # seconds, within a minute -> "00:30.50"

    desc_type = 0  # Text
    desc = SELFID_DESC

    operator_location_type = 1  # Live GNSS (Dynamic)
    classification_type = 1     # EU
    operator_latitude = 52.0
    operator_longitude = 4.5
    area_count = 5
    area_radius = 100
    area_ceiling = 120.0
    area_floor = 20.0
    category_eu = 2  # Specific
    class_eu = 3      # Class 2
    operator_altitude_geo = 110.0
    system_timestamp = 12345678

    operator_id_type = 0
    operator_id = OPERATOR_ID_STR

    auth_type = 1  # UAS ID Signature
    auth_last_page_index = 1
    auth_length = 17 + len(AUTH_PAGE1_DATA)  # 22
    auth_timestamp = 1000

    @classmethod
    def payload(cls):
        return (
            PayloadBuilder()
            .basic_id(0, cls.ua_type_0, cls.id_type_0, BASIC_ID0_STR)
            .basic_id(1, cls.ua_type_1, cls.id_type_1, BASIC_ID1_STR)
            .location(
                status=cls.status, direction=cls.direction,
                speed_horizontal=cls.speed_horizontal,
                speed_vertical=cls.speed_vertical, latitude=cls.latitude,
                longitude=cls.longitude, altitude_baro=cls.altitude_baro,
                altitude_geo=cls.altitude_geo, height_type=cls.height_type,
                height=cls.height, horiz_accuracy=cls.horiz_accuracy,
                vert_accuracy=cls.vert_accuracy,
                baro_accuracy=cls.baro_accuracy,
                speed_accuracy=cls.speed_accuracy,
                ts_accuracy=cls.ts_accuracy,
                timestamp=cls.location_timestamp,
            )
            .self_id(desc_type=cls.desc_type, desc=cls.desc)
            .system(
                operator_location_type=cls.operator_location_type,
                classification_type=cls.classification_type,
                operator_latitude=cls.operator_latitude,
                operator_longitude=cls.operator_longitude,
                area_count=cls.area_count, area_radius=cls.area_radius,
                area_ceiling=cls.area_ceiling, area_floor=cls.area_floor,
                category_eu=cls.category_eu, class_eu=cls.class_eu,
                operator_altitude_geo=cls.operator_altitude_geo,
                timestamp=cls.system_timestamp,
            )
            .operator_id(operator_id_type=cls.operator_id_type,
                         operator_id=cls.operator_id)
            .auth_page(0, auth_type=cls.auth_type, data=AUTH_PAGE0_DATA,
                       last_page_index=cls.auth_last_page_index,
                       length=cls.auth_length, timestamp=cls.auth_timestamp)
            .auth_page(1, auth_type=cls.auth_type, data=AUTH_PAGE1_DATA)
            .valid_flags(basic_id0=1, basic_id1=1, location=1,
                         auth=[1, 1] + [0] * 12, self_id=1, system=1,
                         operator_id=1)
            .build()
        )

    @classmethod
    def valid_blocks(cls):
        vb = open_drone_id_valid_blocks()
        open_drone_id.decode_valid_blocks(cls.payload(), vb)
        return vb

    @classmethod
    def system_timestamp_str(cls):
        return datetime.fromtimestamp(
            cls.system_timestamp + GPS_EPOCH, tz=timezone.utc
        ).strftime('%Y-%m-%d %H:%M %Z')

    @classmethod
    def auth_timestamp_str(cls):
        return datetime.fromtimestamp(
            cls.auth_timestamp + GPS_EPOCH, tz=timezone.utc
        ).strftime('%Y-%m-%d %H:%M %Z')


@pytest.fixture
def golden():
    return Golden

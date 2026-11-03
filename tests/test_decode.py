import base64
import math

import pytest

from conftest import PayloadBuilder, BASIC_ID0_STR, OPERATOR_ID_STR, AUTH_PAGE0_DATA
from modules import decode


def test_decode_uas_data_golden_fields(golden):
    uas = decode.decode_uas_data(golden.payload())

    b0, b1 = uas.basic_id
    assert b0.valid and b1.valid
    assert (b0.ua_type, b0.id_type) == (golden.ua_type_0, golden.id_type_0)
    assert (b1.ua_type, b1.id_type) == (golden.ua_type_1, golden.id_type_1)
    assert b0.id_raw == BASIC_ID0_STR.encode('ascii')
    assert b0.is_text and b1.is_text

    loc = uas.location
    assert loc.valid
    assert loc.status == golden.status
    assert loc.direction == pytest.approx(golden.direction)
    assert loc.speed_horizontal == pytest.approx(golden.speed_horizontal)
    assert loc.speed_vertical == pytest.approx(golden.speed_vertical)
    assert loc.latitude == pytest.approx(golden.latitude)
    assert loc.longitude == pytest.approx(golden.longitude)
    assert loc.altitude_geo == pytest.approx(golden.altitude_geo)
    assert loc.height_type == golden.height_type
    assert loc.timestamp == pytest.approx(golden.location_timestamp)
    assert loc.has_position

    sys_ = uas.system
    assert sys_.valid
    assert sys_.operator_latitude == pytest.approx(golden.operator_latitude)
    assert sys_.operator_longitude == pytest.approx(golden.operator_longitude)
    assert (sys_.area_count, sys_.area_radius) == (golden.area_count, golden.area_radius)
    assert (sys_.category_eu, sys_.class_eu) == (golden.category_eu, golden.class_eu)
    assert sys_.timestamp == golden.system_timestamp
    assert sys_.has_operator_position

    assert uas.self_id.valid and uas.self_id.desc_type == golden.desc_type
    assert uas.operator_id.valid and uas.operator_id.id_text == OPERATOR_ID_STR

    assert len(uas.auth) == 14
    assert uas.auth[0].valid and uas.auth[1].valid and not uas.auth[2].valid
    assert uas.auth[0].length == golden.auth_length
    assert uas.auth[0].last_page_index == golden.auth_last_page_index
    assert uas.auth[0].timestamp == golden.auth_timestamp
    assert uas.auth[0].data_raw[:17] == AUTH_PAGE0_DATA


def test_out_of_range_values_become_nan():
    payload = (PayloadBuilder()
               .location(direction=361.0, speed_horizontal=300.0, speed_vertical=70.0,
                         latitude=0.0, longitude=200.0, altitude_baro=-1000.0,
                         altitude_geo=40000.0, height=-1000.0)
               .system(operator_latitude=95.0, operator_longitude=0.0,
                       area_ceiling=-1000.0, area_floor=-1000.0, operator_altitude_geo=-1000.0)
               .valid_flags(location=1, system=1)
               .build())
    uas = decode.decode_uas_data(payload)
    loc = uas.location
    for value in (loc.direction, loc.speed_horizontal, loc.speed_vertical, loc.latitude,
                  loc.longitude, loc.altitude_baro, loc.altitude_geo, loc.height):
        assert math.isnan(value)
    assert not loc.has_position
    s = uas.system
    for value in (s.operator_latitude, s.operator_longitude, s.area_ceiling, s.area_floor,
                  s.operator_altitude_geo):
        assert math.isnan(value)
    assert not s.has_operator_position


def test_invalid_flags_are_reported():
    uas = decode.decode_uas_data(PayloadBuilder().build())
    assert not any(b.valid for b in uas.basic_id)
    assert not uas.location.valid and not uas.system.valid
    assert not uas.self_id.valid and not uas.operator_id.valid
    assert not any(a.valid for a in uas.auth)
    assert not uas.location.has_position


def test_short_payload_raises():
    with pytest.raises(ValueError):
        decode.decode_uas_data(bytes(100))


def test_serial_number_and_id_text_stop_at_first_nul():
    serial = "1581F5FJD228400MAB"
    padded = (serial + "\x00" * 21)[:21]
    payload = (PayloadBuilder().basic_id(0, 2, 1, padded).valid_flags(basic_id0=1).build())
    uas = decode.decode_uas_data(payload)
    assert uas.basic_id[0].id_text == serial
    assert uas.serial_number == serial


def test_serial_number_taken_from_basic_id_1_when_0_is_no_serial():
    payload = (PayloadBuilder()
               .basic_id(0, 2, 3, "U" * 21)                       # UTM UUID, not a serial
               .basic_id(1, 2, 1, ("SN42" + "\x00" * 21)[:21])
               .valid_flags(basic_id0=1, basic_id1=1).build())
    assert decode.decode_uas_data(payload).serial_number == "SN42"


def test_serial_number_empty_without_valid_serial():
    payload = PayloadBuilder().basic_id(0, 2, 1, "X" * 21).build()   # not valid
    assert decode.decode_uas_data(payload).serial_number == ''


def test_id_text_of_binary_id_is_empty():
    b = decode.BasicID(valid=True, ua_type=0, id_type=3, id_raw=bytes(range(128, 149)))
    assert not b.is_text
    assert b.id_text == ''


def _data_json(golden, **overrides):
    data = {
        'UASdata': base64.b64encode(golden.payload()).decode('ascii'),
        'raw': base64.b64encode(b'\x01\x02').decode('ascii'),
        'sensor ID': 'DS-TEST-1', 'RSSI': -60, 'channel': 6,
        'timestamp': 1700000000000, 'MAC address': 'AA:BB:CC:DD:EE:FF', 'type': 'WLAN Beacon',
    }
    data.update(overrides)
    return data


def test_decode_remote_id_header_fields(golden):
    data = _data_json(golden, extra={'SN present': 1, 'manufacturer': 'Acme'})
    msg = decode.decode_remote_id(data)
    assert msg.sensor_id == 'DS-TEST-1'
    assert msg.rssi == -60
    assert msg.channel == 6
    assert msg.timestamp_ms == 1700000000000
    assert msg.mac_address == 'AA:BB:CC:DD:EE:FF'
    assert msg.type == 'WLAN Beacon'
    assert msg.raw == b'\x01\x02'
    assert msg.extra['manufacturer'] == 'Acme'
    assert msg.json is data
    assert msg.uas.basic_id[0].valid


def test_decode_remote_id_without_raw_and_extra(golden):
    data = _data_json(golden)
    del data['raw']
    data['extra'] = None
    msg = decode.decode_remote_id(data)
    assert msg.raw is None
    assert msg.extra == {}


def test_decode_remote_id_without_uasdata_raises(golden):
    data = _data_json(golden)
    del data['UASdata']
    with pytest.raises(Exception):
        decode.decode_remote_id(data)


@pytest.mark.parametrize("raw, expected", [
    (b'\x8d\x48\x49\xeb', b'\x8d\x48\x49\xeb'),
    (b'\x8d\x48\x49\xeb\x00', b'\x8d\x48\x49\xeb'),
    (b'\x8d\x48\x49\xeb\n', b'\x8d\x48\x49\xeb'),
    (b'', b''),
])
def test_decode_aircraft_strips_one_terminator_from_raw(raw, expected):
    msg = decode.decode_aircraft({'raw': base64.b64encode(raw).decode('ascii')})
    assert msg.raw == expected


def test_decode_aircraft_bad_raw_is_empty():
    assert decode.decode_aircraft({'raw': None}).raw == b''
    assert decode.decode_aircraft({}).raw == b''


@pytest.mark.parametrize("sbs, expected", [
    ("MSG,1,a;MSG,3,b", ["MSG,1,a", "MSG,3,b"]),
    ("MSG,1,a;MSG,3,b\n", ["MSG,1,a", "MSG,3,b"]),
    ("MSG,1,a;MSG,3,b\x00", ["MSG,1,a", "MSG,3,b"]),
    ("MSG,1,a;", ["MSG,1,a", ""]),
    ("", []),
    (None, []),
])
def test_decode_aircraft_sbs_lines(sbs, expected):
    data = {'raw': ''}
    if sbs is not None:
        data['SBS'] = sbs
    assert decode.decode_aircraft(data).sbs_lines == expected


def test_aircraft_message_accessors():
    data = {'raw': '', 'sensor ID': 'DS-1', 'address': '4849EB', 'flight state': 1}
    msg = decode.decode_aircraft(data)
    assert msg.sensor_id == 'DS-1'
    assert msg.address == '4849EB'
    assert msg.get('flight state') == 1
    assert msg.get('missing', 'x') == 'x'


def test_nan_to():
    assert decode.nan_to(float('nan'), 0.0) == 0.0
    assert decode.nan_to(1.5, 0.0) == 1.5
    assert decode.nan_to(None, 0.0) is None

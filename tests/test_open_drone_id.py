import base64
import struct as _struct
from datetime import datetime, timezone

import pytest

from modules import open_drone_id as odid


# ---------------------------------------------------------------------------
# Enum decode functions: table-driven, including out-of-range / reserved
# values so a regression that narrows a table's range is caught.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    (0, "None"), (1, "Serial Number"), (2, "CAA Registration ID"),
    (3, "UTM Assigned UUID"), (4, "specific session ID"),
])
def test_decode_basicID_ID_type(value, expected):
    assert odid.decode_basicID_ID_type(value) == expected


def test_decode_basicID_ID_type_unknown_returns_empty():
    assert odid.decode_basicID_ID_type(99) == ""


@pytest.mark.parametrize("value,expected", [
    (0, "None"), (1, "Aeroplane"), (2, "Helicopter (or Multirotor)"),
    (3, "Gyroplane"), (4, "Hybrid Lift"), (5, "Ornithopter"), (6, "Glider"),
    (7, "Kite"), (8, "Free Balloon"), (9, "Captive Balloon"),
    (10, "Airship (such as a blimp)"),
    (11, "Free Fall/Parachute (unpowered)"), (12, "Rocket"),
    (13, "Tethered Powered Aircraft"), (14, "Ground Obstacle"),
    (15, "Other"),
])
def test_decode_basicID_UA_type(value, expected):
    assert odid.decode_basicID_UA_type(value) == expected


@pytest.mark.parametrize("value,expected", [
    (0, "Undeclared"), (1, "Ground"), (2, "Airborne"), (3, "Emergency"),
    (4, "Remote ID System Failure"),
])
def test_decode_location_status(value, expected):
    assert odid.decode_location_status(value) == expected


@pytest.mark.parametrize("value,expected", [
    (0, "Above Takeoff"), (1, "Above Ground Level"),
])
def test_decode_location_height_type(value, expected):
    assert odid.decode_location_height_type(value) == expected


@pytest.mark.parametrize("value,expected", [
    (0, "Unknown"),
    (1, "10 NM (18.52 km)"),
    (9, "30 m"),
    (10, "10 m"),
    (12, "1 m"),
])
def test_decode_location_horiz_accuracy(value, expected):
    assert odid.decode_location_horiz_accuracy(value) == expected


def test_decode_location_horiz_accuracy_reserved():
    assert odid.decode_location_horiz_accuracy(13) == "Reserved (13)"


@pytest.mark.parametrize("value,expected", [
    (0, "Unknown"), (1, "150 m"), (4, "10 m"), (6, "1 m"),
])
def test_decode_location_vert_accuracy(value, expected):
    assert odid.decode_location_vert_accuracy(value) == expected


def test_decode_location_vert_accuracy_reserved():
    assert odid.decode_location_vert_accuracy(7) == "Reserved (7)"


@pytest.mark.parametrize("value,expected", [
    (0, "Unknown"), (1, "10 m/s"), (2, "3 m/s"), (3, "1 m/s"),
    (4, "0.3 m/s"),
])
def test_decode_location_speed_accuracy(value, expected):
    assert odid.decode_location_speed_accuracy(value) == expected


def test_decode_location_speed_accuracy_reserved():
    assert odid.decode_location_speed_accuracy(5) == "Reserved (5)"


@pytest.mark.parametrize("value,expected", [
    (0, "Unknown"), (1, "0.1 s"), (10, "1.0 s"), (15, "1.5 s"),
])
def test_decode_location_ts_accuracy(value, expected):
    assert odid.decode_location_ts_accuracy(value) == expected


def test_decode_location_ts_accuracy_reserved():
    assert odid.decode_location_ts_accuracy(16) == "Reserved (16)"


@pytest.mark.parametrize("value,expected", [
    (0, "None"), (1, "UAS ID Signature"), (2, "Operator ID Signature"),
    (3, "Message Set Signature"), (4, "Network Remote ID"),
    (5, "Specific Authentication Method"),
])
def test_decode_auth_type(value, expected):
    assert odid.decode_auth_type(value) == expected


@pytest.mark.parametrize("value,expected", [
    (6, "Reserved (6)"), (9, "Reserved (9)"),
    (10, "Private Use (10)"), (15, "Private Use (15)"),
])
def test_decode_auth_type_reserved_and_private(value, expected):
    assert odid.decode_auth_type(value) == expected


@pytest.mark.parametrize("value,expected", [
    (0, "Undefined"), (1, "Open"), (2, "Specific"), (3, "Certified"),
])
def test_decode_system_ua_category(value, expected):
    assert odid.decode_system_ua_category(value) == expected


@pytest.mark.parametrize("value,expected", [
    (0, "Undefined"), (1, "Class 0"), (2, "Class 1"), (3, "Class 2"),
    (4, "Class 3"), (5, "Class 4"), (6, "Class 5"), (7, "Class 6"),
])
def test_decode_system_ua_class(value, expected):
    assert odid.decode_system_ua_class(value) == expected


@pytest.mark.parametrize("value,expected", [
    (0, "Take Off"), (1, "Dynamic"), (2, "Fixed"),
])
def test_decode_system_operator_location_type(value, expected):
    assert odid.decode_system_operator_location_type(value) == expected


@pytest.mark.parametrize("value,expected", [
    (0, "Undeclared"), (1, "European Union"),
])
def test_decode_system_classification_type(value, expected):
    assert odid.decode_system_classification_type(value) == expected


@pytest.mark.parametrize("value,expected", [
    (0, "Text"), (1, "Emergency"), (2, "Extended Status"),
])
def test_decode_selfID_type(value, expected):
    assert odid.decode_selfID_type(value) == expected


def test_decode_operatorID_type():
    assert odid.decode_operatorID_type(0) == "Operator ID"
    assert odid.decode_operatorID_type(1) == ""


def test_clean_string_strips_whitespace_chars():
    assert odid.clean_string("A B\tC\nD\rE") == "ABCDE"


def test_location_decode_speed_horizontal_unknown():
    assert odid.location_decode_speed_horizontal(255, 0) != odid.location_decode_speed_horizontal(255, 0)  # NaN != NaN


def test_location_decode_speed_horizontal_multiplier():
    assert odid.location_decode_speed_horizontal(10, 1) == pytest.approx((10 * 0.75) + (255 * 0.25))
    assert odid.location_decode_speed_horizontal(10, 0) == pytest.approx(10 * 0.75)


# ---------------------------------------------------------------------------
# decode_valid_blocks
# ---------------------------------------------------------------------------

def test_decode_valid_blocks_reads_correct_offsets():
    class Blocks:
        AuthValid = [0] * 16

    buf = bytearray(914)
    buf[892] = 1  # BasicID0_valid
    buf[893] = 0  # BasicID1_valid
    buf[894] = 1  # LocationValid
    buf[895] = 1  # AuthValid[0]
    buf[908] = 1  # AuthValid[13]
    buf[911] = 1  # SelfIDValid
    buf[912] = 0  # SystemValid
    buf[913] = 1  # OperatorIDValid

    blocks = Blocks()
    odid.decode_valid_blocks(bytes(buf), blocks)

    assert blocks.BasicID0_valid == 1
    assert blocks.BasicID1_valid == 0
    assert blocks.LocationValid == 1
    assert blocks.AuthValid[0] == 1
    assert blocks.AuthValid[13] == 1
    assert blocks.AuthValid[1:13] == [0] * 12
    assert blocks.SelfIDValid == 1
    assert blocks.SystemValid == 0
    assert blocks.OperatorIDValid == 1


def test_decode_valid_blocks_independent_across_two_instances():
    """Regression guard: AuthValid is a class-level list on
    open_drone_id_valid_blocks in mqtt_sub.py. If a future edit makes
    decode_valid_blocks only conditionally set AuthValid entries, two
    instances built from different payloads must not leak state into
    each other for the indices that ARE always set (0-13).
    """
    from mqtt_sub import open_drone_id_valid_blocks

    buf_a = bytearray(914)
    buf_a[895] = 1
    a = open_drone_id_valid_blocks()
    odid.decode_valid_blocks(bytes(buf_a), a)

    buf_b = bytearray(914)
    buf_b[895] = 0
    b = open_drone_id_valid_blocks()
    odid.decode_valid_blocks(bytes(buf_b), b)

    assert a.AuthValid[0] == 1
    assert b.AuthValid[0] == 0


# ---------------------------------------------------------------------------
# print_basicID0 / print_basicID1 (also covers the historical UAType_1
# vs IDType_1 mixup, mirrored in log_remote_id.py)
# ---------------------------------------------------------------------------

def test_print_basicID0_ascii_id(payload_builder, capsys):
    payload = payload_builder.basic_id(0, 2, 1, ("SN" + "1" * 30)[:21]).build()
    odid.print_basicID0(payload)
    out = capsys.readouterr().out
    assert "Helicopter (or Multirotor)" in out
    assert "Serial Number" in out
    assert ("SN" + "1" * 30)[:21] in out


def test_print_basicID1_uses_idtype_not_uatype(payload_builder, capsys):
    # UAType_1 == 3 (not 1 or 2); IDType_1 == 2 (is 1 or 2) -> must decode as ASCII
    id_str = ("CAAID" + "9" * 30)[:21]
    payload = payload_builder.basic_id(1, 3, 2, id_str).build()
    odid.print_basicID1(payload)
    out = capsys.readouterr().out
    assert "Gyroplane" in out
    assert "CAA Registration ID" in out
    assert id_str in out
    # must NOT have fallen through to hex encoding of the ascii bytes
    assert id_str.encode('ascii').hex() not in out


def test_print_basicID1_hex_id_when_idtype_not_ascii(payload_builder, capsys):
    raw_id = bytes(range(21))
    payload = bytearray(payload_builder.basic_id(1, 1, 3, "x" * 21).build())
    # overwrite with raw (non-ascii-safe) bytes for the ID field
    offset = 32 + 8
    payload[offset:offset + 21] = raw_id
    odid.print_basicID1(bytes(payload))
    out = capsys.readouterr().out
    assert raw_id.hex() in out


# ---------------------------------------------------------------------------
# print_Location
# ---------------------------------------------------------------------------

def test_print_Location_normal_values(payload_builder, capsys):
    payload = payload_builder.location(
        status=2, direction=45.0, speed_horizontal=12.5, speed_vertical=-2.0,
        latitude=52.379189, longitude=4.899431, altitude_baro=100.5,
        altitude_geo=105.25, height_type=1, height=50.0, horiz_accuracy=10,
        vert_accuracy=4, baro_accuracy=4, speed_accuracy=2, ts_accuracy=5,
        timestamp=30.5,
    ).build()
    odid.print_Location(payload)
    out = capsys.readouterr().out
    assert "Airborne" in out
    assert "45.0" in out
    assert "12.5" in out
    assert "52.379189" in out
    assert "4.899431" in out
    assert "Above Ground Level" in out
    assert "10 m" in out  # horiz accuracy
    assert "0.5 s" in out  # ts accuracy
    assert "00:30.50" in out


def test_print_Location_clamps_invalid_lat_lon_to_nan(payload_builder, capsys):
    payload = payload_builder.location(latitude=0.0, longitude=200.0).build()
    odid.print_Location(payload)
    out = capsys.readouterr().out
    assert "Latitude...... nan" in out
    assert "Longitude...... nan" in out


def test_print_Location_invalid_timestamp(payload_builder, capsys):
    payload = payload_builder.location(timestamp=0.0).build()
    odid.print_Location(payload)
    out = capsys.readouterr().out
    assert "Timestamp......invalid" in out


# ---------------------------------------------------------------------------
# print_SelfID / print_System / print_OperatorID
# ---------------------------------------------------------------------------

def test_print_SelfID(payload_builder, capsys):
    desc = ("Emergency landing" + " " * 30)[:23]
    payload = payload_builder.self_id(desc_type=1, desc=desc).build()
    odid.print_SelfID(payload)
    out = capsys.readouterr().out
    assert "Emergency" in out
    assert desc in out


def test_print_System_valid_timestamp(payload_builder, capsys):
    payload = payload_builder.system(
        operator_location_type=1, classification_type=1,
        operator_latitude=52.0, operator_longitude=4.5, area_count=5,
        area_radius=100, area_ceiling=120.0, area_floor=20.0,
        category_eu=2, class_eu=3, operator_altitude_geo=110.0,
        timestamp=12345678,
    ).build()
    odid.print_System(payload)
    out = capsys.readouterr().out
    assert "Dynamic" in out
    assert "European Union" in out
    assert "Specific" in out
    assert "Class 2" in out
    expected_ts = datetime.fromtimestamp(12345678 + 1546300800, tz=timezone.utc).strftime('%Y-%m-%d %H:%M %Z')
    assert expected_ts in out


def test_print_System_invalid_timestamp(payload_builder, capsys):
    payload = payload_builder.system(timestamp=0).build()
    odid.print_System(payload)
    out = capsys.readouterr().out
    assert "Timestamp......invalid" in out


def test_print_OperatorID(payload_builder, capsys):
    op_id = ("OP" + "5" * 30)[:20]
    payload = payload_builder.operator_id(operator_id_type=0, operator_id=op_id).build()
    odid.print_OperatorID(payload)
    out = capsys.readouterr().out
    assert "Operator ID" in out
    assert op_id in out


# ---------------------------------------------------------------------------
# print_AuthPage
# ---------------------------------------------------------------------------

def test_print_AuthPage_page0_and_last_page(payload_builder, capsys):
    page0_data = bytes(range(17))
    page1_data = bytes(range(5))
    builder = (
        payload_builder
        .auth_page(0, auth_type=1, data=page0_data, last_page_index=1,
                   length=17 + len(page1_data), timestamp=1000)
        .auth_page(1, auth_type=1, data=page1_data)
    )
    payload = builder.build()

    odid.print_AuthPage(payload, 0)
    out0 = capsys.readouterr().out
    assert "UAS ID Signature" in out0
    assert page0_data.hex() in out0
    expected_ts = datetime.fromtimestamp(1000 + 1546300800, tz=timezone.utc).strftime('%Y-%m-%d %H:%M %Z')
    assert expected_ts in out0

    odid.print_AuthPage(payload, 1)
    out1 = capsys.readouterr().out
    assert page1_data.hex() in out1
    # must be truncated to (Length-17)%23 == 5 bytes, not the full 23-byte page
    assert bytes(range(5)).hex() in out1


# ---------------------------------------------------------------------------
# print_payload: full integration over a golden, fully-valid message
# ---------------------------------------------------------------------------

def test_print_payload_full_message(golden, capsys):
    payload = golden.payload()
    valid_blocks = golden.valid_blocks()
    raw_bytes = b'some raw sensor bytes'
    data_json = {
        'sensor ID': 'DS-TEST-1',
        'RSSI': -55,
        'channel': 6,
        'timestamp': 1700000000000,
        'MAC address': 'AA:BB:CC:DD:EE:FF',
        'type': 'WLAN Beacon',
        'raw': base64.b64encode(raw_bytes).decode('ascii'),
    }
    extra_json = {
        'SN present': 1, 'SN valid': 1, 'manufacturer': 'Acme',
        'model': 'Falcon', 'type': 'multirotor', 'application': 'mapping',
        'weight': 1.5, 'dimensions': '400x400x150',
    }

    odid.print_payload(payload, valid_blocks, data_json, extra_json)
    out = capsys.readouterr().out

    assert "DS-TEST-1" in out
    assert raw_bytes.hex() in out  # regression check for the dead `raw` bug
    assert "Helicopter (or Multirotor)" in out  # BasicID0
    assert "Gyroplane" in out  # BasicID1
    assert "Airborne" in out  # Location
    assert "Acme" in out  # extra
    assert "UAS ID Signature" in out  # Auth page 0

import base64
import types

import pytest

from modules import aircraft


@pytest.mark.parametrize("value,expected", [(0, "ground"), (1, "air")])
def test_decode_flight_state(value, expected):
    assert aircraft.decode_flight_state(value) == expected


def test_decode_flight_state_unknown():
    assert aircraft.decode_flight_state(9) == "unknown"


@pytest.mark.parametrize("value,expected", [(0, "none"), (1, "emergency")])
def test_decode_emergency_status(value, expected):
    assert aircraft.decode_emergency_status(value) == expected


def test_decode_emergency_status_unknown():
    assert aircraft.decode_emergency_status(9) == "unknown"


@pytest.mark.parametrize("value,expected", [(1, "baro altitude"), (2, "GNSS height")])
def test_decode_altitude_type(value, expected):
    assert aircraft.decode_altitude_type(value) == expected


def test_decode_altitude_type_unknown():
    assert aircraft.decode_altitude_type(0) == "unknown"


@pytest.mark.parametrize("value,expected", [
    (0, "ICAO address via ADS-B"),
    (1, "reserved (national use)"),
    (2, "ICAO address via TIS-B"),
    (3, "TIS-B track file address"),
    (4, "vehicle address"),
    (5, "fixed ADS-B Beacon Address"),
    (6, "reserved (6)"),
    (7, "reserved (7)"),
])
def test_decode_address_type(value, expected):
    assert aircraft.decode_address_type(value) == expected


@pytest.mark.parametrize("technology,category,expected", [
    ("ADSB", 0, "no information"),
    ("ADSB", 7, "rotorcraft"),
    ("UAT", 14, "UAV"),
    ("ADSB", 8, "reserved (8)"),
    ("ADSB", 21, "line obstacle"),
])
def test_decode_emitter_category(technology, category, expected):
    assert aircraft.decode_emitter_category(category, technology) == expected


def test_decode_emitter_category_unknown_technology():
    assert aircraft.decode_emitter_category(1, "SOMETHING_ELSE") == "unknown"


# ---------------------------------------------------------------------------
# print_payload
# ---------------------------------------------------------------------------

def _aircraft_json(**overrides):
    base = {
        'raw': base64.b64encode(b'\x01\x02\x03').decode('ascii'),
        'sensor ID': 'DS-TEST-1',
        'timestamp': 1700000000000,
        'frequency MHz': 1090.0,
        'type': 'ADSB',
        'address': '4A1B2C',
        'address type': 0,
        'flight': 'KLM123',
        'emergency status': 0,
        'flight state': 1,
        'category': 7,
        'altitude': 3500,
        'altitude type': 1,
        'speed': 120.0,
        'track': 270.0,
        'vertical rate': 0.0,
        'message count': 42,
        'latitude': 52.3,
        'longitude': 4.9,
    }
    base.update(overrides)
    return base


def test_print_payload_prints_decoded_fields(capsys):
    config = types.SimpleNamespace(print_messages=True)
    aircraft.print_payload(_aircraft_json(), config)
    out = capsys.readouterr().out
    assert "4A1B2C" in out
    assert "ICAO address via ADS-B" in out
    assert "rotorcraft" in out
    assert "baro altitude" in out
    assert "air" in out
    assert "none" in out


def test_print_payload_non_icao_address_masked(capsys):
    config = types.SimpleNamespace(print_messages=True)
    aircraft.print_payload(_aircraft_json(**{'address type': 1, 'address': '4A1B2C'}), config)
    out = capsys.readouterr().out
    assert "FF1B2C" in out


def test_print_payload_zero_lat_lon_prints_na(capsys):
    config = types.SimpleNamespace(print_messages=True)
    aircraft.print_payload(_aircraft_json(latitude=0.0, longitude=0.0), config)
    out = capsys.readouterr().out
    assert "latitude............. N/A" in out
    assert "longitude............ N/A" in out


def test_print_payload_suppressed_when_disabled(capsys):
    config = types.SimpleNamespace(print_messages=False)
    aircraft.print_payload(_aircraft_json(), config)
    out = capsys.readouterr().out
    assert out == ""


# ---------------------------------------------------------------------------
# CSV export round trip
# ---------------------------------------------------------------------------

def test_open_csv_write_csv_round_trip(tmp_path):
    filename = aircraft.open_csv(str(tmp_path))
    aircraft.write_csv(_aircraft_json(), filename)

    with open(filename, newline='', encoding='utf-8') as f:
        import csv
        rows = list(csv.reader(f))

    header, data_row = rows[0], rows[1]
    assert len(header) == len(data_row)
    assert 'DS-TEST-1' in data_row
    assert '4A1B2C' in data_row

import xml.etree.ElementTree as ET

import pytest

from tests.conftest import PayloadBuilder, make_rid_msg, BASIC_ID0_STR, _no_exporters_left_over, golden   # noqa: F401 (fixtures)
from modules import decode
from integrations.tak import cot

NOW = 1793712471.0                     # 2026-11-03 13:27:51 UTC
TS_MS = 1793712470500


def _rid(payload, **data):
    base = {'timestamp': TS_MS, 'sensor ID': 'ds0948', 'RSSI': -60, 'type': 'WLAN Beacon',
            'MAC address': 'AA:BB:CC:DD:EE:FF'}
    base.update(data)
    return make_rid_msg(base, payload)


def _parse(data):
    assert isinstance(data, bytes)
    return ET.fromstring(data)


def _aircraft(**overrides):
    a = {'raw': '', 'sensor ID': 'ds0948', 'timestamp': TS_MS, 'frequency MHz': 1090, 'RSSI': -64.9,
         'type': 'ADSB', 'address': '4849EB', 'address type': 0, 'flight': 'PHTWK   ', 'emergency status': 0,
         'flight state': 1, 'category': 2, 'altitude': 1402, 'altitude type': 1, 'speed': 64, 'track': 210,
         'vertical rate': 0, 'latitude': 52.16774116, 'longitude': 6.91573661}
    a.update(overrides)
    return decode.decode_aircraft(a)


# --- drone -----------------------------------------------------------------

def test_drone_event(golden):
    e = _parse(cot.drone_event(_rid(golden.payload()), now=NOW))
    assert e.tag == 'event' and e.get('version') == '2.0'
    assert e.get('uid') == 'dronescout-rid-' + BASIC_ID0_STR
    assert e.get('type') == 'a-u-A-M-F-Q'
    assert e.get('how') == 'm-g'
    assert e.get('time') == e.get('start') == '2026-11-03T13:27:50.500Z'   # sensor time
    assert e.get('stale') == '2026-11-03T13:28:01.000Z'                    # gateway time + 10 s
    p = e.find('point')
    assert float(p.get('lat')) == pytest.approx(golden.latitude)
    assert float(p.get('lon')) == pytest.approx(golden.longitude)
    assert float(p.get('hae')) == pytest.approx(golden.altitude_geo)
    assert float(p.get('ce')) == 10.0          # horizontal accuracy code 10 = 10 m
    assert float(p.get('le')) == 10.0          # vertical accuracy code 4 = 10 m
    assert e.find('detail/contact').get('callsign') == BASIC_ID0_STR
    t = e.find('detail/track')
    assert float(t.get('speed')) == pytest.approx(golden.speed_horizontal)
    assert float(t.get('course')) == pytest.approx(golden.direction)
    remarks = e.find('detail/remarks').text
    for text in ('Serial Number: ' + BASIC_ID0_STR, 'Helicopter', 'Airborne', 'EU Specific, Class 2',
                 'operator ID: OPID-', 'self-ID: Flying over the park', 'RSSI -60 dBm', 'sensor ds0948'):
        assert text in remarks


def test_drone_without_position_gives_no_event(golden):
    payload = (PayloadBuilder().basic_id(0, 2, 1, BASIC_ID0_STR).location(latitude=0.0, longitude=0.0)
               .valid_flags(basic_id0=1, location=1).build())
    assert cot.drone_event(_rid(payload), now=NOW) is None
    payload = PayloadBuilder().basic_id(0, 2, 1, BASIC_ID0_STR).valid_flags(basic_id0=1).build()
    assert cot.drone_event(_rid(payload), now=NOW) is None     # location not valid


def test_drone_unknown_accuracy_and_height():
    payload = (PayloadBuilder().basic_id(0, 2, 1, BASIC_ID0_STR)
               .location(latitude=52.0, longitude=6.0, altitude_geo=-1000.0)
               .valid_flags(basic_id0=1, location=1).build())
    p = _parse(cot.drone_event(_rid(payload), now=NOW)).find('point')
    assert p.get('hae') == p.get('ce') == p.get('le') == '9999999'


def test_drone_without_velocity_has_no_track():
    payload = (PayloadBuilder().basic_id(0, 2, 1, BASIC_ID0_STR)
               .location(latitude=52.0, longitude=6.0, speed_horizontal=0.0, direction=0.0)
               .valid_flags(basic_id0=1, location=1).build())
    assert _parse(cot.drone_event(_rid(payload), now=NOW)).find('detail/track') is None


def test_drone_id_falls_back_to_other_id_then_mac():
    caa = ("NLD-OP-123" + "\x00" * 21)[:21]
    payload = (PayloadBuilder().basic_id(0, 2, 2, caa).location(latitude=52.0, longitude=6.0)
               .valid_flags(basic_id0=1, location=1).build())
    assert cot.drone_id(_rid(payload)) == "NLD-OP-123"
    payload = PayloadBuilder().location(latitude=52.0, longitude=6.0).valid_flags(location=1).build()
    msg = _rid(payload)
    assert cot.drone_id(msg) == "AABBCCDDEEFF"
    assert cot.drone_uid(msg) == "dronescout-rid-AABBCCDDEEFF"


def test_special_characters_are_escaped():
    desc = ('<b>&"x"' + ' ' * 23)[:23]
    payload = (PayloadBuilder().basic_id(0, 2, 1, BASIC_ID0_STR).location(latitude=52.0, longitude=6.0)
               .self_id(desc=desc).valid_flags(basic_id0=1, location=1, self_id=1).build())
    e = _parse(cot.drone_event(_rid(payload), now=NOW))       # must still be valid XML
    assert '<b>&"x"' in e.find('detail/remarks').text


def test_custom_types_and_stale(golden):
    e = _parse(cot.drone_event(_rid(golden.payload()), {'type_drone': 'a-h-A-M-F-Q', 'stale_s': 30}, now=NOW))
    assert e.get('type') == 'a-h-A-M-F-Q'
    assert e.get('stale') == '2026-11-03T13:28:21.000Z'


# --- pilot -----------------------------------------------------------------

def test_operator_event(golden):
    e = _parse(cot.operator_event(_rid(golden.payload()), now=NOW))
    assert e.get('uid') == 'dronescout-op-' + BASIC_ID0_STR
    assert e.get('type') == 'a-u-G'
    p = e.find('point')
    assert float(p.get('lat')) == pytest.approx(golden.operator_latitude)
    assert float(p.get('lon')) == pytest.approx(golden.operator_longitude)
    link = e.find('detail/link')
    assert link.get('uid') == 'dronescout-rid-' + BASIC_ID0_STR
    assert link.get('relation') == 'p-p'
    assert 'operator ID: OPID-' in e.find('detail/remarks').text


def test_operator_without_position_gives_no_event(golden):
    payload = PayloadBuilder().basic_id(0, 2, 1, BASIC_ID0_STR).valid_flags(basic_id0=1).build()
    assert cot.operator_event(_rid(payload), now=NOW) is None


# --- aircraft --------------------------------------------------------------

def test_aircraft_event_phtwk():
    e = _parse(cot.aircraft_event(_aircraft(), now=NOW))
    assert e.get('uid') == 'icao-4849EB'
    assert e.get('type') == 'a-n-A-C-F'
    p = e.find('point')
    assert (p.get('lat'), p.get('lon'), p.get('hae')) == ('52.16774116', '6.91573661', '1402')
    assert e.find('detail/contact').get('callsign') == 'PHTWK'
    t = e.find('detail/track')
    assert (t.get('speed'), t.get('course')) == ('64', '210')
    remarks = e.find('detail/remarks').text
    for text in ('ADSB 1090 MHz', 'address 4849EB', 'flight PHTWK', 'baro altitude', 'air', 'RSSI -64.9 dBm'):
        assert text in remarks


@pytest.mark.parametrize("technology, category, expected", [
    ('ADSB', 1, 'a-n-A-C-F'), ('ADSB', 5, 'a-n-A-C-F'), ('UAT', 9, 'a-n-A-C-F'),
    ('ADSB', 7, 'a-n-A-C-H'), ('ADSB', 10, 'a-n-A-C-L'), ('ADSB', 14, 'a-u-A-M-F-Q'),
    ('ADSB', 8, 'a-n-A-C'), ('ADSB', 0, 'a-n-A-C'), ('ADSB', None, 'a-n-A-C'),
    ('ADSL', 1, 'a-n-A-C'), ('FLARM', 7, 'a-n-A-C'),
])
def test_aircraft_type(technology, category, expected):
    assert cot.aircraft_type(_aircraft(type=technology, category=category)) == expected


def test_aircraft_uid_for_non_icao_addresses():
    assert cot.aircraft_uid(_aircraft(**{'address type': 2})) == 'icao-4849EB'      # ICAO via TIS-B
    assert cot.aircraft_uid(_aircraft(**{'address type': 3})) == 'dronescout-adsb-4849EB'
    assert cot.aircraft_uid(_aircraft(type='FLARM', address='dd1234', **{'address type': 9})) == 'dronescout-flarm-DD1234'
    assert cot.aircraft_uid(_aircraft(type='SOME TYPE', **{'address type': 12})) == 'dronescout-some-type-4849EB'


def test_aircraft_without_position_gives_no_event():
    assert cot.aircraft_event(_aircraft(latitude=0.0, longitude=0.0), now=NOW) is None
    assert cot.aircraft_event(_aircraft(latitude=None), now=NOW) is None


def test_aircraft_before_velocity_message_has_no_track():
    # like 4864EB in the 1090 dump of 2026-11-03: speed/track 0 until the first velocity message
    e = _parse(cot.aircraft_event(_aircraft(speed=0, track=0, flight=''), now=NOW))
    assert e.find('detail/track') is None
    assert e.find('detail/contact').get('callsign') == '4849EB'


def test_aircraft_emergency_and_unknown_altitude():
    e = _parse(cot.aircraft_event(_aircraft(**{'emergency status': 1, 'altitude': None}), now=NOW))
    assert 'EMERGENCY' in e.find('detail/remarks').text
    assert e.find('point').get('hae') == '9999999'


# --- sensor / ping ---------------------------------------------------------

def test_sensor_event():
    status = {'sensor ID': 'ds0948', 'timestamp': TS_MS, 'status': 'degraded', 'status details': 'wlan2 stalled',
              'WLAN radios': {'wlan1': 'ok', 'wlan2': 'stalled'}, 'SDR receivers': {'1090': 'ok'},
              'model': 'ds240', 'firmware version': '20261103'}
    e = _parse(cot.sensor_event('ds0948', (52.2185, 6.9748, 30.0), status, now=NOW))
    assert e.get('uid') == 'dronescout-ds0948'
    assert e.get('type') == 'a-f-G-E-S'
    assert e.get('stale') == '2026-11-03T13:42:51.000Z'      # 900 s
    assert e.find('point').get('hae') == '30'
    remarks = e.find('detail/remarks').text
    for text in ('status: degraded', 'wlan2 stalled', 'wlan1 ok', '1090 ok', 'ds240 20261103'):
        assert text in remarks


def test_sensor_without_position_gives_no_event():
    assert cot.sensor_event('ds0948', None, {'status': 'OK'}, now=NOW) is None


def test_sensor_without_status_or_altitude():
    e = _parse(cot.sensor_event('ds0948', (52.2, 6.9), None, now=NOW))
    assert e.find('point').get('hae') == '9999999'
    assert e.find('detail/remarks').text == 'DroneScout sensor ds0948'


def test_ping_event():
    e = _parse(cot.ping_event('dronescout-gateway', now=NOW))
    assert e.get('type') == 't-x-c-t'
    assert e.get('uid') == 'dronescout-gateway-ping'
    assert e.get('how') == 'h-g-i-g-o'

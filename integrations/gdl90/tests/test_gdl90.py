import socket

import pytest

from tests.conftest import _no_exporters_left_over   # noqa: F401 (autouse fixture)
from modules import decode, test_objects, tcp_sbs_export
from integrations.gdl90 import gdl90_export, gdl90_map as gm

CENTER = (52.22, 6.89)
T0 = 1_760_000_000.0


# --- examples from the GDL 90 Data Interface Specification (560-1058-00 Rev A) ------------------

def test_crc_of_the_specification_example():
    # section 2.2.4: heartbeat 7E 00 81 41 DB D0 08 02 B3 8B 7E
    msg = bytes.fromhex('008141dbd00802')
    assert gm.crc16(msg) == 0x8BB3
    assert gm.frame(msg) == bytes.fromhex('7e008141dbd00802b38b7e')


def test_traffic_report_of_the_specification_example():
    # section 3.5.4: N825V, 44.90708 -122.99488, 5000 ft, 123 kt, +64 fpm, track 45, light, NIC 10 NACp 9
    example = bytes.fromhex('1400ab45491fef15a889780f09a907b00120014e3832355620202000')
    r = gm.parse_report(example)
    assert (r['lat'], r['lon']) == pytest.approx((44.90708, -122.99488), abs=2e-5)
    assert (r['alt_ft'], r['speed_kt'], r['vs_fpm'], r['track'], r['callsign']) == (5000, 123, 64, 45.0, 'N825V')
    encoded = gm._report(gm.MSG_TRAFFIC, 0, 0xAB4549, r['lat'], r['lon'], 5000 * gm.FT, True, 10, 9,
                         123 * gm.KT, 64 * gm.FPM, 45.0, 1, 'N825V')
    assert encoded == example


def test_escaping():
    msg = bytes((gm.MSG_TRAFFIC, 0x7E, 0x7D, 0x01))
    framed = gm.frame(msg)
    assert framed[0] == framed[-1] == 0x7E and 0x7E not in framed[1:-1]
    assert gm.unframe(framed) == [msg]
    assert gm.unframe(framed[:-3] + b'\x00' + framed[-2:]) == []          # bad CRC


def test_heartbeat():
    now = 1_700_000_000 + 0.5          # 22:13:20 UTC = 80000 s: bit 16 set
    hb = gm.heartbeat(now, gps_valid=True, message_count=3)
    assert hb[0] == 0 and hb[1] == 0x81 and hb[2] == 0x81
    assert (hb[3] | hb[4] << 8 | (hb[2] >> 7) << 16) == 80000
    assert hb[5:] == b'\x00\x03'


def test_foreflight_id():
    m = gm.foreflight_id(name='DroneSct', long_name='DroneScout')
    assert len(m) == 39 and m[:3] == b'\x65\x00\x01' and m[11:19] == b'DroneSct'


# --- mapping ------------------------------------------------------------------------------------

def test_drone_traffic():
    msg = test_objects.drone(T0, CENTER)
    r = gm.parse_report(gm.drone_traffic(msg))
    loc = msg.uas.location
    assert r['address'] == int(decode.pseudo_icao(decode.drone_id(msg)), 16) and r['address_type'] == 1
    assert r['emitter'] == gm.EMITTER_UAV and r['callsign'] == 'DRONRONE'
    assert (r['lat'], r['lon']) == pytest.approx((loc.latitude, loc.longitude), abs=3e-5)
    assert r['alt_ft'] == 400                          # baro 120 m = 394 ft, 25 ft steps
    assert r['speed_kt'] == round(loc.speed_horizontal / gm.KT)
    assert r['nacp'] == r['nic'] == 10 and r['misc'] == 0x09        # airborne, true track


def test_drone_address_and_callsign_match_the_sbs_output():
    msg = test_objects.drone(T0, CENTER)
    serial = msg.uas.serial_number
    r = gm.parse_report(gm.drone_traffic(msg))
    assert '%06X' % r['address'] == tcp_sbs_export.ICAO(serial)
    assert r['callsign'] == tcp_sbs_export.callsign(serial).strip()


def test_drone_without_baro_uses_geodetic_altitude_and_unknown_values():
    msg = test_objects.drone(T0, CENTER)
    loc = msg.uas.location
    loc.altitude_baro = loc.speed_horizontal = loc.speed_vertical = float('nan')
    loc.status = 1
    r = gm.parse_report(gm.drone_traffic(msg, nic=0))
    assert r['alt_ft'] == round(165 / gm.FT / 25) * 25
    assert r['speed_kt'] is None and r['vs_fpm'] is None and r['misc'] == 0 and r['nic'] == 0


def test_drone_without_position():
    msg = test_objects.drone(T0, CENTER)
    msg.uas.location.valid = False
    assert gm.drone_traffic(msg) is None


def test_aircraft_traffic():
    r = gm.parse_report(gm.aircraft_traffic(test_objects.aircraft(T0, CENTER)))
    assert r['address'] == 0xFFFFF1 and r['address_type'] == gm.ADDR_ADSB_SELF_ASSIGNED   # address type 12
    assert r['alt_ft'] == 2950 and r['speed_kt'] == 117 and r['emitter'] == 1
    assert r['callsign'] == 'DRONESCO' and r['misc'] == 0x09


def test_aircraft_icao_on_ground_emergency():
    msg = decode.decode_aircraft({'raw': '', 'sensor ID': 's', 'timestamp': 1, 'type': 'ADSB', 'address': '4840D6',
                                  'address type': 0, 'flight': 'KLM1234 ', 'emergency status': 1, 'flight state': 0,
                                  'category': 3, 'altitude': -10, 'altitude type': 1, 'speed': 5, 'track': 270,
                                  'vertical rate': -2.5, 'latitude': 52.3, 'longitude': 4.76})
    r = gm.parse_report(gm.aircraft_traffic(msg))
    assert r['address'] == 0x4840D6 and r['address_type'] == gm.ADDR_ADSB_ICAO
    assert r['misc'] & 0x08 == 0 and r['emergency'] == 1 and r['emitter'] == 3
    assert r['alt_ft'] == -25 and r['vs_fpm'] == -512


def test_aircraft_without_position():
    msg = decode.decode_aircraft({'raw': '', 'sensor ID': 's', 'timestamp': 1, 'type': 'ADSB', 'address': 'abc123',
                                  'latitude': 0, 'longitude': 0})
    assert gm.aircraft_traffic(msg) is None


# --- exporter -------------------------------------------------------------------------------------

@pytest.fixture
def listener():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('127.0.0.1', 0))
    s.settimeout(2)
    yield s
    s.close()


def receive_all(sock):
    out = []
    sock.settimeout(0.2)
    try:
        while True:
            out += gm.unframe(sock.recvfrom(65535)[0])
    except socket.timeout:
        return out


def test_exporter_sends_one_message_per_datagram(listener):
    exp = gdl90_export.GDL90Exporter([listener.getsockname()], start=False)
    exp.on_remote_id(test_objects.drone(T0, CENTER))
    exp.on_remote_id(test_objects.drone(T0 + 0.5, CENTER))           # newest replaces the first
    exp.on_aircraft(test_objects.aircraft(T0, CENTER))
    assert exp.flush() == 2
    ids = [m[0] for m in receive_all(listener)]
    assert ids == [gm.MSG_HEARTBEAT, gm.MSG_FOREFLIGHT, gm.MSG_TRAFFIC, gm.MSG_TRAFFIC]
    assert exp.sent == 4
    assert exp.flush() == 0                                          # nothing new: heartbeat + ID only
    assert [m[0] for m in receive_all(listener)] == [gm.MSG_HEARTBEAT, gm.MSG_FOREFLIGHT]
    exp.close()


def test_ownship_from_a_sensor(listener):
    exp = gdl90_export.GDL90Exporter([listener.getsockname()], ownship_sensor='ds1', start=False)
    exp.on_location({'sensor ID': 'other', 'latitude': 1.0, 'longitude': 1.0})
    assert exp.ownship is None
    exp.on_location({'sensor ID': 'ds1', 'latitude': 52.0, 'longitude': 6.0, 'altitude MSL': 30.0})
    exp.flush()
    messages = receive_all(listener)
    assert messages[0][1] & 0x80                                     # heartbeat: GPS valid
    own = gm.parse_report(next(m for m in messages if m[0] == gm.MSG_OWNSHIP))
    assert (own['lat'], own['lon']) == pytest.approx((52.0, 6.0), abs=3e-5) and own['misc'] & 0x08 == 0
    assert any(m[0] == gm.MSG_OWNSHIP_GEO_ALT for m in messages)
    exp.close()


def test_filters(listener):
    exp = gdl90_export.GDL90Exporter([listener.getsockname()], send_drones=False, send_aircraft=False, start=False)
    exp.on_remote_id(test_objects.drone(T0, CENTER))
    exp.on_aircraft(test_objects.aircraft(T0, CENTER))
    assert exp.flush() == 0
    exp.close()


def test_gateway_builds_exporter_from_gdl90_config(monkeypatch, listener):
    from integrations.gdl90 import gdl90_config, gdl90_gateway
    monkeypatch.setattr(gdl90_config, 'gdl90_destinations', ['127.0.0.1:%d' % listener.getsockname()[1]])
    monkeypatch.setattr(gdl90_config, 'gdl90_ownship', (52.0, 6.0, 10.0), raising=False)
    exp = gdl90_gateway.make_exporter(gdl90_config, start=False)
    try:
        assert exp.ownship == (52.0, 6.0, 10.0) and exp.device_name == 'DroneSct'
        exp.flush()
        assert gm.MSG_OWNSHIP in [m[0] for m in receive_all(listener)]
    finally:
        exp.close()

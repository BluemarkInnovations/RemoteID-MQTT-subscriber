import socket
import time

import pytest

from tests.conftest import _no_exporters_left_over   # noqa: F401 (autouse fixture)
from modules import decode, test_objects
from integrations.flarm import flarm_export, flarm_map as fm

CENTER = (52.22, 6.89)
T0 = 1_760_000_000.0
OWN = (52.21, 6.88, 10.0, 0.0)


class FakeClock:
    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t


# --- sentences --------------------------------------------------------------------------------

def test_checksum_of_a_known_sentence():
    # well-known NMEA example sentence
    assert fm.checksum('GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,') == 0x47
    assert fm.sentence('PFLAU,0,1,2,1,0,,0,,,') == '$PFLAU,0,1,2,1,0,,0,,,*4C\r\n'


def test_parse_sentence_checks_the_checksum():
    line = fm.pflau(3, True)
    assert fm.parse_sentence(line)[:4] == ['PFLAU', '3', '0', '2']
    assert fm.parse_sentence(line.replace('PFLAU,3', 'PFLAU,4')) is None
    assert fm.parse_sentence('PFLAU,3') is None


def test_gps_sentences():
    rmc = fm.parse_sentence(fm.gprmc(52.21852, -6.97486, now=1_700_000_000))
    assert rmc[1] == '221320.00' and rmc[2] == 'A' and rmc[9] == '141123'
    assert (rmc[3], rmc[4], rmc[5], rmc[6]) == ('5213.1112', 'N', '00658.4916', 'W')
    gga = fm.parse_sentence(fm.gpgga(-33.5, 151.25, 30.0, now=1_700_000_000))
    assert (gga[2], gga[3], gga[4], gga[5], gga[6], gga[9]) == ('3330.0000', 'S', '15115.0000', 'E', '1', '30.0')


def test_relative_position():
    north, east = fm.relative_position(52.0, 6.0, 52.01, 6.0)
    assert north == pytest.approx(1112, abs=1) and east == pytest.approx(0, abs=0.01)
    north, east = fm.relative_position(52.0, 6.0, 52.0, 6.01)
    assert east == pytest.approx(685, abs=1)                # cos(52 deg) * 1112 m


# --- targets ----------------------------------------------------------------------------------

def test_drone_target():
    msg = test_objects.drone(T0, CENTER)
    f = fm.parse_sentence(fm.drone_target(msg, OWN))
    north, east = fm.relative_position(OWN[0], OWN[1], msg.uas.location.latitude, msg.uas.location.longitude)
    assert f[0] == 'PFLAA' and f[1] == '0'
    assert (int(f[2]), int(f[3])) == (round(north), round(east))
    assert f[4] == '120'                                     # height above ground, sensor on the ground
    assert f[5] == str(fm.ID_RANDOM) and f[6] == decode.pseudo_icao(decode.drone_id(msg))
    assert f[11] == fm.ACFT_UAV and f[9] == '16'             # 15.7 m/s


def test_drone_vertical_fallbacks():
    msg = test_objects.drone(T0, CENTER)
    loc = msg.uas.location
    loc.height_type = 0                                      # above takeoff: not used
    assert fm.parse_sentence(fm.drone_target(msg, OWN))[4] == '110'      # baro 120 - 10 m
    loc.altitude_baro = float('nan')
    assert fm.parse_sentence(fm.drone_target(msg, OWN))[4] == '155'      # geodetic 165 - 10 m
    loc.altitude_geo = float('nan')
    assert fm.parse_sentence(fm.drone_target(msg, OWN))[4] == ''
    assert fm.parse_sentence(fm.drone_target(msg, (52.21, 6.88, 10.0, 5.0)))[4] == ''
    loc.height_type = 1
    assert fm.parse_sentence(fm.drone_target(msg, (52.21, 6.88, 10.0, 5.0)))[4] == '115'   # antenna 5 m AGL


def test_unknown_speed_leaves_track_and_speed_empty():
    msg = test_objects.drone(T0, CENTER)
    msg.uas.location.speed_horizontal = msg.uas.location.speed_vertical = float('nan')
    f = fm.parse_sentence(fm.drone_target(msg, OWN))
    assert f[7] == f[9] == f[10] == '' and f[8] == ''


def test_aircraft_target_types():
    def target(**kw):
        d = {'raw': '', 'sensor ID': 's', 'timestamp': 1, 'type': 'ADSB', 'address': '4840d6', 'address type': 0,
             'category': 3, 'altitude': 1010, 'speed': 100, 'track': 359.6, 'vertical rate': 40,
             'latitude': 52.3, 'longitude': 6.9}
        d.update(kw)
        return fm.parse_sentence(fm.aircraft_target(decode.decode_aircraft(d), OWN))
    f = target()
    assert f[5] == str(fm.ID_ICAO) and f[6] == '4840D6' and f[11] == fm.ACFT_JET
    assert f[4] == '1000' and f[7] == '0' and f[10] == '32.7'          # track 359.6 -> 0, climb capped
    assert target(category=7)[11] == fm.ACFT_HELICOPTER
    assert target(category=9)[11] == fm.ACFT_GLIDER
    assert target(category=12)[11] == fm.ACFT_UNKNOWN
    assert target(**{'address type': 9})[5] == str(fm.ID_FLARM)
    assert target(**{'address type': 12})[5] == str(fm.ID_RANDOM)
    assert fm.aircraft_target(decode.decode_aircraft({'raw': '', 'latitude': 0, 'longitude': 0}), OWN) is None


# --- exporter ----------------------------------------------------------------------------------

def test_no_position_no_traffic():
    logs = []
    exp = flarm_export.FLARMExporter(tcp_port=None, udp_destinations=[('127.0.0.1', 9)], start=False,
                                     log=logs.append)
    exp.on_remote_id(test_objects.drone(T0, CENTER))
    lines = exp.lines()
    assert len(lines) == 1 and fm.parse_sentence(lines[0])[3] == '0'      # PFLAU, GPS 0
    assert 'no own position' in logs[0]
    exp.on_location({'sensor ID': 'ds1', 'latitude': 52.0, 'longitude': 6.0})
    assert exp.position is None                              # not the configured sensor
    exp.close()


def test_position_from_sensor_range_and_stale():
    clock = FakeClock(100.0)
    exp = flarm_export.FLARMExporter(tcp_port=None, udp_destinations=[('127.0.0.1', 9)], position_sensor='ds1',
                                     max_range_m=5000, stale_s=5, clock=clock, start=False, log=lambda *a: None)
    exp.on_location({'sensor ID': 'ds1', 'latitude': CENTER[0], 'longitude': CENTER[1], 'altitude MSL': 10})
    exp.on_remote_id(test_objects.drone(T0, CENTER))
    exp.on_aircraft(test_objects.aircraft(T0 + 242, CENTER))             # 2.2 km north, 0.5 km west
    exp.on_aircraft(decode.decode_aircraft({'raw': '', 'sensor ID': 's', 'type': 'ADSB', 'address': 'AAAAAA',
                                            'latitude': CENTER[0] + 0.5, 'longitude': CENTER[1]}))   # 55 km
    names = [fm.parse_sentence(line)[0] for line in exp.lines()]
    assert names == ['GPRMC', 'GPGGA', 'PFLAU', 'PFLAA', 'PFLAA'] and exp.out_of_range == 1
    clock.t = 106.0
    assert [fm.parse_sentence(line)[0] for line in exp.lines()] == ['GPRMC', 'GPGGA', 'PFLAU']
    exp.close()


def test_tcp_clients_get_the_sentences():
    exp = flarm_export.FLARMExporter(tcp_port=0, tcp_bind='127.0.0.1', position=(52.2, 6.9, 10.0),
                                     interval_s=3600, log=lambda *a: None)
    try:
        clients = [socket.create_connection(('127.0.0.1', exp.tcp_port), timeout=2) for _ in range(2)]
        deadline = time.time() + 2
        while exp.clients < 2 and time.time() < deadline:
            time.sleep(0.01)
        exp.on_remote_id(test_objects.drone(T0, (52.2, 6.9)))
        exp.flush()
        for c in clients:
            data = b''
            while data.count(b'\n') < 4:
                data += c.recv(4096)
            lines = data.decode().splitlines()
            assert [fm.parse_sentence(line)[0] for line in lines] == ['GPRMC', 'GPGGA', 'PFLAU', 'PFLAA']
        clients[0].close()
        time.sleep(0.05)
        exp.flush()
        exp.flush()                                          # the closed client is noticed and dropped
        assert exp.clients == 1
        clients[1].close()
    finally:
        exp.close()


def test_udp_datagram_holds_one_second():
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.bind(('127.0.0.1', 0))
    rx.settimeout(2)
    exp = flarm_export.FLARMExporter(tcp_port=None, udp_destinations=[rx.getsockname()], position=(52.2, 6.9, 10.0),
                                     start=False)
    try:
        exp.on_aircraft(test_objects.aircraft(T0, (52.2, 6.9)))
        assert exp.flush() == 4
        lines = rx.recvfrom(65535)[0].decode().splitlines()
        assert [fm.parse_sentence(line)[0] for line in lines] == ['GPRMC', 'GPGGA', 'PFLAU', 'PFLAA']
    finally:
        exp.close()
        rx.close()


def test_needs_tcp_or_udp():
    with pytest.raises(ValueError):
        flarm_export.FLARMExporter(tcp_port=None, start=False)


def test_gateway_builds_exporter_from_flarm_config(monkeypatch):
    from integrations.flarm import flarm_config, flarm_gateway
    monkeypatch.setattr(flarm_config, 'flarm_tcp_port', 0)
    monkeypatch.setattr(flarm_config, 'flarm_tcp_bind', '127.0.0.1')
    monkeypatch.setattr(flarm_config, 'flarm_position', (52.0, 6.0, 5.0), raising=False)
    exp = flarm_gateway.make_exporter(flarm_config, start=False)
    try:
        assert exp.position == (52.0, 6.0, 5.0) and exp.max_range_m == 50000 and exp.tcp_port > 0
    finally:
        exp.close()

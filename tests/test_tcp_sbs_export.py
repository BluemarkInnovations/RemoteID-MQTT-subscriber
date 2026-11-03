from modules import tcp_sbs_export as sbs


def test_ICAO_is_deterministic_and_prefixed_with_FF():
    a = sbs.ICAO("SN1234567890123456")
    b = sbs.ICAO("SN1234567890123456")
    assert a == b
    assert a.startswith("FF")
    assert len(a) == 6  # "FF" + 2-byte checksum as hex


def test_ICAO_known_values():
    # Locks in the XOR-checksum algorithm itself, not just determinism.
    assert sbs.ICAO("A") == "FF0531"
    assert sbs.ICAO("SN12345678901234567") == "FF5567"


def test_ICAO_differs_for_different_input():
    assert sbs.ICAO("DRONE0001") != sbs.ICAO("DRONE0002")


def test_callsign_short_string_not_padded():
    # Documents current behavior: only strings > 8 chars go through
    # first4+last4 and end up 8 chars; shorter strings pass through as-is.
    assert sbs.callsign("SN123") == "SN123"


def test_callsign_exactly_eight_chars_unchanged():
    assert sbs.callsign("12345678") == "12345678"


def test_callsign_long_string_uses_manufacturer_and_serial_prefix_suffix():
    result = sbs.callsign("MANUFACTURERSN12345678")
    assert result == "MANU5678"
    assert len(result) == 8


# --- export of decoded RemoteID / aircraft messages --------------------------

import math
import socket

import pytest

from conftest import PayloadBuilder
from modules import decode


class FakeConn:
    def __init__(self, fail=False):
        self.lines = []
        self.fail = fail
        self.closed = False

    def sendall(self, data):
        if self.fail:
            raise BrokenPipeError("client gone")
        self.lines.append(data.decode('ascii'))

    def setsockopt(self, *args):
        pass

    def close(self):
        self.closed = True


@pytest.fixture
def conn(monkeypatch):
    c = FakeConn()
    monkeypatch.setattr(sbs, 'sbs_connection', c)
    return c


SERIAL = "1581F5FJD228400MAB"


def _uas(status, id_type=1, latitude=52.2, longitude=6.9, location_valid=1, **loc):
    payload = (PayloadBuilder()
               .basic_id(0, 2, id_type, (SERIAL + "\x00" * 21)[:21])
               .location(status=status, latitude=latitude, longitude=longitude,
                         altitude_geo=loc.get('altitude_geo', 100.0),
                         speed_horizontal=loc.get('speed', 10.0), direction=loc.get('direction', 90.0))
               .valid_flags(basic_id0=1, location=location_valid)
               .build())
    return decode.decode_uas_data(payload)


def test_export_airborne_sends_msg_1_3_4(conn):
    sbs.export(_uas(status=2))
    icao = sbs.ICAO(SERIAL)
    assert len(conn.lines) == 3
    assert conn.lines[0] == "MSG,1,,," + icao + ",,,,,," + sbs.callsign(SERIAL) + ",,,,,,,,0,0,0,0\n"
    assert conn.lines[1].startswith("MSG,3,,," + icao + ",,,,,,,,,90,")
    assert conn.lines[1].endswith(",,,0,0,0,0\n")            # airborne: not on ground
    assert conn.lines[2] == "MSG,4,,," + icao + ",,,,,,,328,33,90,,,,0,0,0,0\n"


def test_export_ground_sends_msg_2_with_altitude_and_speed(conn):
    sbs.export(_uas(status=1))
    assert len(conn.lines) == 2
    assert conn.lines[1].startswith("MSG,2,,," + sbs.ICAO(SERIAL) + ",,,,,,,328,33,90,52.2")
    assert conn.lines[1].endswith(",,,0,0,0,1\n")            # on ground flag


@pytest.mark.parametrize("status", [3, 4])
def test_export_emergency_flag(conn, status):
    sbs.export(_uas(status=status))
    assert conn.lines[1].endswith(",,,0,1,0,1\n")


def test_export_unknown_position_sent_as_zero(conn):
    sbs.export(_uas(status=2, latitude=0.0, longitude=200.0, altitude_geo=-1000.0))
    fields = conn.lines[1].split(',')
    assert fields[14:16] == ['0.0', '0.0']
    assert conn.lines[2].split(',')[11] == '0'                 # altitude


def test_export_without_location_sends_only_msg_1(conn):
    sbs.export(_uas(status=2, location_valid=0))
    assert len(conn.lines) == 1 and conn.lines[0].startswith("MSG,1,")


def test_export_skips_drone_without_serial_number(conn):
    sbs.export(_uas(status=2, id_type=2))                      # CAA registration ID
    assert conn.lines == []


def test_export_without_client_sends_nothing(monkeypatch):
    monkeypatch.setattr(sbs, 'sbs_connection', None)
    sbs.export(_uas(status=2))                                 # must not raise


def test_transmit_drops_disconnected_client(monkeypatch, capsys):
    c = FakeConn(fail=True)
    monkeypatch.setattr(sbs, 'sbs_connection', c)
    sbs.transmit("MSG,1\n")
    assert sbs.sbs_connection is None
    assert c.closed
    assert "SBS client disconnected" in capsys.readouterr().out
    sbs.transmit("MSG,1\n")                                    # no client: silently ignored


def test_sbs_exporter_remote_id(conn, golden):
    from conftest import make_rid_msg
    payload = (PayloadBuilder().basic_id(0, 2, 1, (SERIAL + "\x00" * 21)[:21])
               .valid_flags(basic_id0=1).build())
    sbs.SBSExporter().on_remote_id(make_rid_msg({'timestamp': 1700000000000}, payload))
    assert len(conn.lines) == 1 and conn.lines[0].startswith("MSG,1,,," + sbs.ICAO(SERIAL))


def test_sbs_exporter_aircraft_sends_sensor_lines(conn):
    msg = decode.decode_aircraft({'raw': '', 'SBS': 'MSG,1,a;MSG,3,b\n'})
    sbs.SBSExporter().on_aircraft(msg)
    assert conn.lines == ['MSG,1,a\n', 'MSG,3,b\n']


def test_sbs_listener_serves_one_client_and_accepts_the_next(monkeypatch):
    import threading, time
    s = socket.socket(); s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]; s.close()
    monkeypatch.setattr(sbs.config, 'sbs_server_ip_address', '127.0.0.1', raising=False)
    monkeypatch.setattr(sbs.config, 'sbs_server_port', port, raising=False)
    monkeypatch.setattr(sbs, 'sbs_connection', None)
    threading.Thread(target=sbs.connect, args=(1,), daemon=True).start()

    def connect_client():
        for _ in range(50):
            try:
                c = socket.create_connection(('127.0.0.1', port), timeout=2)
                break
            except OSError:
                time.sleep(0.05)
        for _ in range(50):
            if sbs.sbs_connection is not None:
                return c
            time.sleep(0.05)
        raise AssertionError("listener did not accept")

    c1 = connect_client()
    sbs.transmit("MSG,1,first\n")
    assert c1.recv(100) == b"MSG,1,first\n"
    c1.close()
    for _ in range(20):                                        # a closed peer fails on a later send
        sbs.transmit("MSG,x\n" * 1000)
        if sbs.sbs_connection is None:
            break
    assert sbs.sbs_connection is None
    c2 = connect_client()
    sbs.transmit("MSG,1,second\n")
    assert c2.recv(100) == b"MSG,1,second\n"
    c2.close()

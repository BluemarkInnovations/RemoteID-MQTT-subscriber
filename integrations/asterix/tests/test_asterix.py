import socket
import time

import pytest

pytest.importorskip('asterix', reason='libasterix (Python >= 3.10) not installed')

from tests.conftest import _no_exporters_left_over   # noqa: F401 (autouse fixture)
from modules import decode, test_objects
from integrations.asterix import asterix_export, asterix_map as am

CENTER = (52.22, 6.89)
NOW = 1_794_480_000.0           # 2026-11-12 10:40:00 UTC


def _aircraft(**overrides):
    a = {'raw': '', 'sensor ID': 'ds0948', 'timestamp': int(NOW * 1000), 'frequency MHz': 1090, 'RSSI': -71.4,
         'type': 'ADSB', 'address': '4849EB', 'address type': 0, 'flight': 'PHTWK   ', 'category': 2,
         'altitude': 1402, 'altitude type': 1, 'speed': 64, 'track': 210, 'vertical rate': -2.5,
         'latitude': 52.16774116, 'longitude': 6.91573661, 'flight state': 1}
    a.update(overrides)
    return decode.decode_aircraft(a)


def _one(category_class, record):
    data = am.datablocks(category_class, [record])[0]
    (cat, recs), = am.parse_datagram(data)
    assert cat == category_class.cv_category
    return recs[0]


def _q(rec, item, sub=None, unit=None):
    v = rec.get_item(item).variation
    if sub:
        v = v.get_item(sub).variation
    return v.content.as_quantity(unit) if unit else v.content.as_uint()


# --- helpers -------------------------------------------------------------------------------

def test_time_of_day():
    assert am.time_of_day(NOW) == 10 * 3600 + 40 * 60


@pytest.mark.parametrize('dronescout, asterix', [(1, 1), (5, 5), (7, 10), (9, 11), (10, 12), (11, 16),
                                                 (12, 15), (14, 13), (15, 14), (17, 20), (21, 24), (8, 0), (None, 0)])
def test_emitter_category_translation(dronescout, asterix):
    assert am.emitter_category(dronescout) == asterix


# --- CAT021 --------------------------------------------------------------------------------

def test_cat021_record():
    rec = _one(am.CAT021, am.cat021_record(_aircraft(), 25, 3, track_number=4100, service_id=1, now=NOW + 0.5))
    assert (_q(rec, '010', 'SAC'), _q(rec, '010', 'SIC')) == (25, 3)
    assert _q(rec, '080') == 0x4849EB
    assert _q(rec, '040', 'ATP') == 0
    assert _q(rec, '161', 'TRNUM') == 4100 & 0xFFF
    assert _q(rec, '131', 'LAT', '°') == pytest.approx(52.16774116, abs=2e-7)
    assert _q(rec, '131', 'LON', '°') == pytest.approx(6.91573661, abs=2e-7)
    assert _q(rec, '145', None, 'FL') == pytest.approx(1402 / 0.3048 / 100, abs=0.25)
    assert rec.get_item('140') is None                                   # baro: no geometric height
    assert _q(rec, '073', None, 's') == pytest.approx(am.time_of_day(NOW), abs=1 / 128)
    assert _q(rec, '077', None, 's') == pytest.approx(am.time_of_day(NOW + 0.5), abs=1 / 128)
    assert _q(rec, '160', 'GS', 'NM/s') * 1852 == pytest.approx(64, abs=0.2)
    assert _q(rec, '160', 'TA', '°') == pytest.approx(210, abs=0.01)
    assert _q(rec, '155', 'BVR', 'ft/min') == pytest.approx(-2.5 / 0.3048 * 60, abs=6.25)
    assert rec.get_item('170').variation.content.as_string().strip() == 'PHTWK'
    assert _q(rec, '020') == 2
    assert _q(rec, '132', None, 'dBm') == -71
    assert _q(rec, '090', 'NUCPNIC') == 0                                 # mandatory, "unknown"


def test_cat021_identification_bytes_match_the_received_frame():
    # I021/170 "PHTWK" as in the ADS-B identification message received on 2026-11-03
    rec = am.cat021_record(_aircraft(), 0, 1)
    assert rec.get_item('170').unparse().to_bytes().hex() == '4085172e0820'


def test_cat021_gnss_height_ground_and_anonymous():
    rec = _one(am.CAT021, am.cat021_record(_aircraft(**{'altitude type': 2, 'flight state': 0, 'address type': 1}), 0, 1))
    assert _q(rec, '140', None, 'ft') == pytest.approx(1402 / 0.3048, abs=6.25)
    assert rec.get_item('145') is None
    assert rec.get_item('157') is not None and rec.get_item('155') is None
    assert _q(rec, '040', 'GBS') == 1
    assert _q(rec, '040', 'ATP') == 3


def test_cat021_unknown_velocity_is_left_out():
    rec = _one(am.CAT021, am.cat021_record(_aircraft(speed=0, track=0, **{'vertical rate': 0}), 0, 1))
    assert rec.get_item('160') is None and rec.get_item('155') is None


@pytest.mark.parametrize('overrides', [{'type': 'FLARM'}, {'latitude': 0, 'longitude': 0}, {'address': 'xyz'}])
def test_cat021_not_for(overrides):
    assert am.cat021_record(_aircraft(**overrides), 0, 1) is None


# --- CAT023 --------------------------------------------------------------------------------

def test_cat023_status():
    status = {'status': 'degraded', 'SDR receivers': {'1090': 'ok', '978': 'missing'}}
    data = am.datablocks(am.CAT023, am.cat023_records(0, 7, status, 10, 1, 100, now=NOW))[0]
    (cat, recs), = am.parse_datagram(data)
    assert [_q(r, '000') for r in recs] == [1, 2, 2]
    assert _q(recs[0], '100', 'NOGO') == 0
    assert _q(recs[0], '100', 'GSSP', 's') == 10
    stat = {_q(r, '015', 'SID'): _q(r, '110', 'STAT') for r in recs[1:]}
    assert stat == {am.SERVICE_1090: 4, am.SERVICE_UAT: 1}        # normal / failed
    assert _q(recs[1], '200', None, 'NM') == 100


def test_cat023_without_status_is_not_operational():
    recs = am.cat023_records(0, 7, None, 10, 1, now=NOW)
    assert _one(am.CAT023, recs[0]).get_item('100').variation.get_item('NOGO').variation.content.as_uint() == 1
    assert _q(_one(am.CAT023, recs[1]), '110', 'STAT') == 0


# --- CAT129 --------------------------------------------------------------------------------

def test_cat129_layout_from_the_specification():
    msg = test_objects.drone(NOW, CENTER)
    r = am.cat129_record(msg, 0x12, 0x34, 'NL', destination=(5, 6), now=NOW, special_purpose=False)
    # FSPEC: FRN 1,2,5,6,7 | 8,9,10,11,14 | 15
    assert r[:3] == bytes([0b11001111, 0b11110011, 0b10000000])
    i = 3
    assert r[i:i + 2] == b'\x12\x34'; i += 2                    # 010
    assert r[i:i + 2] == b'\x05\x06'; i += 2                    # 015
    assert r[i:i + 12] == b'ST-TEST-DRONE'[-12:].rjust(12); i += 12   # 040: last 12 characters
    assert r[i:i + 2] == b'NL'; i += 2                          # 050
    assert int.from_bytes(r[i:i + 3], 'big') == int(am.time_of_day(NOW) * 128); i += 3   # 070
    lat = int.from_bytes(r[i:i + 4], 'big', signed=True) * 180 / 2 ** 30
    lon = int.from_bytes(r[i + 4:i + 8], 'big', signed=True) * 180 / 2 ** 30; i += 8
    assert lat == pytest.approx(msg.uas.location.latitude, abs=2e-7)
    assert lon == pytest.approx(msg.uas.location.longitude, abs=2e-7)
    assert int.from_bytes(r[i:i + 3], 'big', signed=True) == 1200; i += 3   # 090: baro 120.0 m, LSB 0.1 m
    assert int.from_bytes(r[i:i + 3], 'big', signed=True) == 1200; i += 3   # 100: AGL height 120.0 m
    assert int.from_bytes(r[i:i + 2], 'big') == 5; i += 2       # 110: 10 m (95 %) -> CEP50 4.8 m
    assert len(r) == i + 5 + 3                                  # 185 (5) + 220 (3)


def test_cat129_round_trip_and_special_purpose():
    msg = test_objects.drone(NOW, CENTER)
    data = am.raw_datablock(129, [am.cat129_record(msg, 1, 2, now=NOW)])[0]
    assert data[0] == 129 and int.from_bytes(data[1:3], 'big') == len(data)
    (cat, recs), = am.parse_datagram(data)
    rec = recs[0]
    assert rec['country'] == 'ZZ'
    assert rec['SP'] == b'ID=DRONESCOUT-TEST-DRONE'
    loc = msg.uas.location
    assert rec['vx'] ** 2 + rec['vy'] ** 2 == pytest.approx(loc.speed_horizontal ** 2, rel=1e-3)
    assert 'DAC' not in rec


def test_cat129_needs_position_and_altitude():
    msg = test_objects.drone(NOW, CENTER)
    msg.uas.location.altitude_baro = float('nan')
    msg.uas.location.altitude_geo = float('nan')
    msg.uas.location.height_type = 0                  # height above takeoff is not AGL
    assert am.cat129_record(msg, 0, 1) is None
    msg.uas.location.altitude_geo = 50.0              # geodetic as AMSL approximation
    assert am.parse_cat129(am.cat129_record(msg, 0, 1, special_purpose=False))[0]['amsl'] == 50.0
    msg.uas.location.latitude = float('nan')
    assert am.cat129_record(msg, 0, 1) is None


def test_cat129_negative_values():
    msg = test_objects.drone(NOW, (-33.9, -70.6))
    msg.uas.location.altitude_baro = -12.3
    msg.uas.location.speed_vertical = -1.5
    rec = am.parse_cat129(am.cat129_record(msg, 0, 1, special_purpose=False))[0]
    assert rec['lat'] < 0 and rec['lon'] < 0
    assert rec['amsl'] == pytest.approx(-12.3) and rec['vv'] == pytest.approx(-1.5)


# --- CAT062 / CAT065 ------------------------------------------------------------------------

def test_cat062_drone_track():
    msg = test_objects.drone(NOW, CENTER)
    rec = _one(am.CAT062, am.cat062_drone_record(msg, 0, 100, 513, service_id=1, first=True, now=NOW))
    assert _q(rec, '040') == 513
    assert _q(rec, '080', 'TSB') == 1 and _q(rec, '080', 'TSE') == 0 and _q(rec, '080', 'SRC') == 1
    assert _q(rec, '105', 'LAT', '°') == pytest.approx(msg.uas.location.latitude, abs=1e-6)
    assert _q(rec, '130', None, 'ft') == pytest.approx(165 / 0.3048, abs=6.25)
    assert rec.get_item('380').variation.get_item('EMC').variation.content.as_uint() == 13


def test_cat062_aircraft_last_report():
    rec = _one(am.CAT062, am.cat062_aircraft_record(_aircraft(), 0, 100, 7, last=True, now=NOW))
    assert _q(rec, '080', 'TSE') == 1
    assert _q(rec, '136', None, 'FL') == pytest.approx(1402 / 0.3048 / 100, abs=0.25)
    adr = rec.get_item('380').variation
    assert adr.get_item('ADR').variation.content.as_uint() == 0x4849EB
    assert adr.get_item('EMC').variation.content.as_uint() == 2


def test_cat065():
    rec = _one(am.CAT065, am.cat065_record(0, 100, 1, now=NOW))
    assert _q(rec, '000') == 1 and _q(rec, '040', 'NOGO') == 0


def test_datablocks_split_at_max_size():
    recs = [am.cat021_record(_aircraft(address='%06X' % i), 0, 1) for i in range(100)]
    blocks = am.datablocks(am.CAT021, recs, max_bytes=500)
    assert len(blocks) > 1 and all(len(b) <= 500 for b in blocks)
    assert sum(len(r) for _, r in am.parse_datagram(b''.join(blocks))) == 100


# --- exporter -------------------------------------------------------------------------------

@pytest.fixture
def receiver():
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.bind(('127.0.0.1', 0))
    rx.settimeout(0.5)
    yield rx
    rx.close()


def _receive_all(rx):
    out = {}
    while True:
        try:
            data = rx.recv(65535)
        except socket.timeout:
            return out
        for cat, recs in am.parse_datagram(data):
            out.setdefault(cat, []).extend(recs)


def test_exporter_sends_all_categories(receiver):
    logs = []
    t = [0.0]
    exp = asterix_export.AsterixExporter([('127.0.0.1', receiver.getsockname()[1])], 0, {'s1': 1},
                                         categories=asterix_export.ALL_CATEGORIES, gateway_sic=100,
                                         stale_s=5, heartbeat_s=5, cat062_aircraft=True, log=logs.append,
                                         clock=lambda: t[0], start=False)
    try:
        exp.on_status({'sensor ID': 's1', 'status': 'normal'})
        for _ in range(3):                    # three updates of the same objects in one interval
            exp.on_remote_id(_drone('s1'))
            exp.on_aircraft(_aircraft(**{'sensor ID': 's1'}))
        exp.on_aircraft(_aircraft(**{'sensor ID': 'unknown-sensor', 'address': 'ABCDEF'}))
        exp.flush()
        got = _receive_all(receiver)
        assert {c: len(r) for c, r in got.items()} == {21: 1, 129: 1, 62: 3, 23: 3, 65: 1}
        assert sum('unknown-sensor' in line for line in logs) == 1
        t[0] = 6.0                           # nothing new for longer than stale_s
        exp.flush()
        got = _receive_all(receiver)
        assert len(got[62]) == 3 and all(_q(r, '080', 'TSE') == 1 for r in got[62])
        assert 23 in got                     # heartbeat is due again
        t[0] = 7.0
        exp.flush()
        assert _receive_all(receiver) == {}  # no traffic, no heartbeat due
    finally:
        exp.close()


def _drone(sensor_id):
    msg = test_objects.drone(time.time(), CENTER)
    msg.sensor_id = sensor_id
    return msg


def test_exporter_track_numbers_are_stable(receiver):
    exp = asterix_export.AsterixExporter([('127.0.0.1', receiver.getsockname()[1])], 0, {'s1': 1},
                                         categories=(21,), log=lambda *a: None, start=False)
    try:
        for _ in range(2):
            exp.on_aircraft(_aircraft(**{'sensor ID': 's1'}))
            exp.on_aircraft(_aircraft(**{'sensor ID': 's1', 'address': 'ABCDEF'}))
            exp.flush()
        recs = _receive_all(receiver)[21]
        numbers = [_q(r, '161', 'TRNUM') for r in recs]
        assert numbers == [0, 1, 0, 1]
    finally:
        exp.close()


def test_exporter_configuration_errors():
    with pytest.raises(ValueError):
        asterix_export.AsterixExporter([], 0, {}, start=False)
    with pytest.raises(ValueError):
        asterix_export.AsterixExporter([('127.0.0.1', 1)], 0, {}, categories=(48,), start=False)
    with pytest.raises(ValueError):
        asterix_export.AsterixExporter([('127.0.0.1', 1)], 0, {}, categories=(62,), start=False)


def test_parse_destinations():
    assert asterix_export.parse_destinations(['1.2.3.4:8600', ('239.1.2.3', '8601')]) == \
        [('1.2.3.4', 8600), ('239.1.2.3', 8601)]


def test_gateway_builds_exporter_from_config(monkeypatch):
    from integrations.asterix import asterix_config, asterix_gateway
    monkeypatch.setattr(asterix_config, 'asterix_sic_table', {'ds1': 9})
    monkeypatch.setattr(asterix_config, 'asterix_categories', [21, 62, 65])
    exp = asterix_gateway.make_exporter(asterix_config, start=False)
    try:
        assert exp.sic_table == {'ds1': 9} and exp.categories == {21, 62, 65}
        assert exp.gateway_sic == 100 and exp.destinations == [('127.0.0.1', 8600)]
    finally:
        exp.close()

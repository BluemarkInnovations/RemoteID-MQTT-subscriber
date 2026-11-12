import base64
import math

from modules import aircraft, decode, log_remote_id, replay, test_objects

T0 = 1_760_000_000.0
CENTER = (52.22, 6.89)


def _with_json(msg):
    msg.json = {'sensor ID': msg.sensor_id, 'RSSI': -60, 'channel': 6, 'timestamp': msg.timestamp_ms,
                'MAC address': msg.mac_address, 'type': 'WiFi beacon'}
    msg.uas._auth = [decode.AuthPage(False, 0, 0, 0, 0, 0, bytes(23)) for _ in range(decode.AUTH_PAGES)]
    return msg


def test_remote_id_csv_round_trip(tmp_path):
    path = log_remote_id.open_csv(str(tmp_path))
    msgs = [_with_json(test_objects.drone(T0 + i, CENTER)) for i in range(3)]
    for m in msgs:
        log_remote_id.write_csv(m, path)
    replayed = list(replay.read(path))
    assert [ts for ts, _ in replayed] == [m.timestamp_ms for m in msgs]
    for (ts, data), orig in zip(replayed, msgs):
        back = decode.decode_remote_id(data['data'])
        assert back.sensor_id == orig.sensor_id and back.mac_address == orig.mac_address
        assert back.json['type'] == 'WiFi beacon'                       # the 1st "type" column, not the SN lookup's
        assert decode.drone_id(back) == decode.drone_id(orig)
        a, b = back.uas.location, orig.uas.location
        for field in ('latitude', 'longitude', 'altitude_baro', 'altitude_geo', 'height', 'direction',
                      'speed_horizontal', 'timestamp'):
            assert math.isclose(getattr(a, field), getattr(b, field), rel_tol=1e-6), field
        assert back.uas.self_id.desc_raw == orig.uas.self_id.desc_raw
        assert back.uas.operator_id.id_text == orig.uas.operator_id.id_text
        assert back.uas.system.operator_latitude == orig.uas.system.operator_latitude


def test_aircraft_csv_round_trip_keeps_fractions_and_empty_flight(tmp_path):
    path = aircraft.open_csv(str(tmp_path))
    adsl = decode.decode_aircraft({'sensor ID': 's', 'timestamp': 1_760_000_000_500, 'frequency MHz': 868.2,
                                   'RSSI': -80.5, 'type': 'ADSL', 'address': '3F1234', 'address type': 3,
                                   'flight': '', 'altitude': 1200, 'altitude type': 2, 'speed': 5.5,
                                   'track': 90, 'vertical rate': -1.5, 'latitude': 52.0, 'longitude': 6.0,
                                   'raw': base64.b64encode(b'\x01\x02\x00\x00').decode()})
    aircraft.write_csv(adsl, path)
    [(ts, m)] = list(replay.read(path))
    a = m['aircraft']
    assert ts == 1_760_000_000_500 and a['frequency MHz'] == 868.2 and a['speed'] == 5.5
    assert a['vertical rate'] == -1.5 and a['altitude'] == 1200 and isinstance(a['altitude'], int)
    assert a['flight'] == ''
    assert decode.decode_aircraft(a).raw == b'\x01\x02\x00'          # the logged frame, exactly


def test_messages_merges_logs_in_time_order(tmp_path):
    rid = log_remote_id.open_csv(str(tmp_path))
    for i in (0, 2):
        log_remote_id.write_csv(_with_json(test_objects.drone(T0 + i, CENTER)), rid)
    adsb = aircraft.open_csv(str(tmp_path))
    for i in (1, 3):
        aircraft.write_csv(test_objects.aircraft(T0 + 1000 + i, CENTER), adsb)   # another day / hour
    kinds = [next(k for k in m if k != 'protocol') for _, m in replay.messages([rid, adsb])]
    assert kinds == ['data', 'aircraft', 'data', 'aircraft']                    # aligned to a common start
    ts = [t for t, _ in replay.messages([rid, adsb], align=False)]
    assert ts == sorted(ts)


def test_retime():
    m = {'protocol': 1.0, 'data': {'timestamp': 1}}
    assert replay.retime(m, 5)['data']['timestamp'] == 5

import json

import pytest

import config
import mqtt_sub
from modules import exporters
from modules import sensor_locations as sl

SENSOR = 'ds-no-gps'
MANUAL = {SENSOR: (52.1, 6.1, 25.0)}


class Clock:
    def __init__(self, t=1_700_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


class Recorder(exporters.Exporter):
    name = 'recorder'

    def __init__(self):
        self.locations = []

    def on_location(self, location_json):
        self.locations.append(location_json)


def test_valid_fix():
    assert sl.valid_fix({'latitude': 52.0, 'longitude': 6.0, 'satellites': 7})
    assert sl.valid_fix({'latitude': 52.0, 'longitude': 6.0})
    assert not sl.valid_fix({'latitude': 0.0, 'longitude': 0.0})
    assert not sl.valid_fix({'latitude': 52.0, 'longitude': 6.0, 'satellites': 0})
    assert not sl.valid_fix({'latitude': None, 'longitude': 6.0})
    assert not sl.valid_fix({'latitude': float('nan'), 'longitude': 6.0})
    assert not sl.valid_fix({'latitude': 91.0, 'longitude': 6.0})


def test_parse_checks_the_table():
    assert sl.parse({'a': (52, 6)}) == {'a': (52.0, 6.0, 0.0)}
    for bad in ({'a': (52,)}, {'a': 'x'}, {'a': (95, 6, 0)}, {'a': (52, 6, 0, 1)}):
        with pytest.raises(ValueError):
            sl.parse(bad)


def test_manual_position_until_gps_and_after_gps_is_lost():
    clock = Clock()
    locs = sl.SensorLocations(MANUAL, gps_timeout_s=180, interval_s=60, clock=clock)
    first = locs.seen(SENSOR)
    assert first == {'sensor ID': SENSOR, 'timestamp': 1_700_000_000_000, 'latitude': 52.1, 'longitude': 6.1,
                     'altitude MSL': 25.0, 'source': 'manual'}
    clock.t += 30
    assert locs.seen(SENSOR) is None                      # at most once per interval
    clock.t += 30
    assert locs.seen(SENSOR) is not None
    assert locs.seen('other sensor') is None              # no manual position configured
    assert locs.gps({'sensor ID': SENSOR, 'latitude': 52.2, 'longitude': 6.2, 'satellites': 8})
    clock.t += 100
    assert locs.seen(SENSOR) is None                      # GPS wins
    assert not locs.gps({'sensor ID': SENSOR, 'latitude': 0.0, 'longitude': 0.0})   # lost the fix
    clock.t += 81
    assert locs.seen(SENSOR) is not None                  # 181 s without a valid fix: manual again


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(config, 'print_messages', False, raising=False)
    monkeypatch.delattr(config, 'log_path', raising=False)
    clock = Clock()
    monkeypatch.setattr(mqtt_sub, '_sensor_locations', sl.SensorLocations(MANUAL, clock=clock))

    class C:
        def subscribe(self, topic):
            pass
    c = C()
    c.clock = clock
    mqtt_sub.subscribe(c)
    return c


class Msg:
    def __init__(self, *objs):
        self.topic = 'test'
        self.payload = ''.join(json.dumps(o) for o in objs).encode()


def test_mqtt_sub_sends_the_manual_position_to_the_outputs(client):
    rec = exporters.register(Recorder(), threaded=False)
    status = {'protocol': 1.0, 'status': {'sensor ID': SENSOR, 'timestamp': 1, 'status': 'normal'}}
    client.on_message(client, None, Msg(status))
    assert [l['source'] for l in rec.locations] == ['manual'] and rec.locations[0]['latitude'] == 52.1
    client.on_message(client, None, Msg(status))
    assert len(rec.locations) == 1                        # not again within a minute
    other = {'protocol': 1.0, 'status': {'sensor ID': 'with-gps', 'timestamp': 1, 'status': 'normal'}}
    client.on_message(client, None, Msg(other))
    assert len(rec.locations) == 1


def test_mqtt_sub_passes_gps_and_drops_locations_without_fix(client):
    rec = exporters.register(Recorder(), threaded=False)
    no_fix = {'protocol': 1.0, 'location': {'sensor ID': SENSOR, 'timestamp': 1, 'latitude': 0.0,
                                            'longitude': 0.0, 'satellites': 0}}
    client.on_message(client, None, Msg(no_fix))
    assert [l.get('source') for l in rec.locations] == ['manual']           # the fix-less one isn't passed on
    client.clock.t += 61
    fix = {'protocol': 1.0, 'location': {'sensor ID': SENSOR, 'timestamp': 2, 'latitude': 52.3,
                                         'longitude': 6.3, 'satellites': 9}}
    client.on_message(client, None, Msg(fix))
    assert rec.locations[-1]['latitude'] == 52.3 and len(rec.locations) == 2  # GPS, no manual before it

# (c) Bluemark Innovations BV
# MIT license
"""Synthetic test objects for testing the integrations (TAK, SAPIENT, ASTERIX, REST)
against a real server without real detections.

Only made-up data: a drone with its pilot flying a circle, an aircraft flying east
and a sensor. Every ID / callsign contains DRONESCOUT-TEST and the remarks / self-ID
say TEST DATA, so on a shared server nobody takes them for real detections.
"""

import math

from modules import decode

TEST = 'DRONESCOUT-TEST'
TEST_SENSOR = TEST + '-SENSOR'
TEST_AIRCRAFT_ADDRESS = 'FFFFF1'
NAN = float('nan')


def _ascii(text, size):
    return text.encode('ascii')[:size].ljust(size, b'\x00')


def drone(t, center, radius_m=300.0, period_s=120.0):
    """A RemoteIDMessage for a drone flying a circle around center, pilot in the middle."""
    lat0, lon0 = center
    angle = 2 * math.pi * (t % period_s) / period_s
    dlat = radius_m * math.cos(angle) / 111320.0
    dlon = radius_m * math.sin(angle) / (111320.0 * math.cos(math.radians(lat0)))
    course = (math.degrees(angle) + 90.0) % 360.0
    speed = 2 * math.pi * radius_m / period_s
    uas = decode.UASData(
        raw=b'',
        basic_id=[decode.BasicID(True, 2, 1, _ascii(TEST + '-DRONE', 21)),
                  decode.BasicID(False, 0, 0, bytes(21))],
        location=decode.Location(True, 2, course, speed, 0.0, lat0 + dlat, lon0 + dlon, 120.0, 165.0,
                                 1, 120.0, 10, 4, 4, 2, 5, t % 3600),
        self_id=decode.SelfID(True, 0, _ascii('TEST DATA', 23)),
        system=decode.System(True, 1, 1, lat0, lon0, 1, 0, NAN, NAN, 1, 2, 45.0, 0),
        operator_id=decode.OperatorID(True, 0, _ascii(TEST + '-OPERATOR', 20)),
        auth=[],
    )
    return decode.RemoteIDMessage(sensor_id=TEST_SENSOR, timestamp_ms=int(t * 1000), rssi=-60,
                                  channel=6, mac_address='02:00:00:00:00:01', type='TEST DATA',
                                  raw=None, extra={}, uas=uas)


def aircraft(t, center):
    """An AircraftMessage for an aircraft flying east, north of center."""
    lat0, lon0 = center
    x = ((t % 300) - 150) * 60.0                      # 60 m/s, 18 km track
    return decode.decode_aircraft({
        'raw': '', 'sensor ID': TEST_SENSOR, 'timestamp': int(t * 1000), 'frequency MHz': 1090,
        'RSSI': -70, 'type': 'ADSB', 'address': TEST_AIRCRAFT_ADDRESS, 'address type': 12,
        'flight': TEST, 'emergency status': 0, 'flight state': 1, 'category': 1,
        'altitude': 900, 'altitude type': 1, 'speed': 60, 'track': 90, 'vertical rate': 0,
        'latitude': lat0 + 0.02, 'longitude': lon0 + x / (111320.0 * math.cos(math.radians(lat0))),
    })


def sensor(t, center):
    return ({'sensor ID': TEST_SENSOR, 'timestamp': int(t * 1000), 'latitude': center[0] - 0.003,
             'longitude': center[1] - 0.003, 'altitude MSL': 10},
            {'sensor ID': TEST_SENSOR, 'timestamp': int(t * 1000), 'status': 'normal',
             'model': 'TEST DATA', 'firmware version': 'test'})

# (c) Bluemark Innovations BV
# MIT license
"""FLARM NMEA sentences from decoded DroneScout messages.

The FLARM data port (FLARM "Data Port Specification") is what FLARM devices send to
gliding and GA software: XCSoar, LK8000, SeeYou Navigator, SkyDemon, ... Traffic is
sent relative to the own position:

    $PFLAU,<RX>,<TX>,<GPS>,<Power>,<AlarmLevel>,<RelativeBearing>,<AlarmType>,<RelativeVertical>,<RelativeDistance>,<ID>
    $PFLAA,<AlarmLevel>,<RelativeNorth>,<RelativeEast>,<RelativeVertical>,<IDType>,<ID>,<Track>,<TurnRate>,<GroundSpeed>,<ClimbRate>,<AcftType>

The own position is the DroneScout sensor (or a configured position), sent as
$GPRMC / $GPGGA like the GPS sentences of a FLARM device. Pure functions.
"""

import math
import time
from datetime import datetime, timezone

from modules import decode

# FLARM aircraft types (hex digit)
ACFT_UNKNOWN = '0'
ACFT_GLIDER = '1'
ACFT_HELICOPTER = '3'
ACFT_SKYDIVER = '4'
ACFT_HANG_GLIDER = '6'
ACFT_POWERED = '8'
ACFT_JET = '9'
ACFT_BALLOON = 'B'
ACFT_UAV = 'D'
ACFT_STATIC = 'F'

# FLARM ID types
ID_RANDOM = 0
ID_ICAO = 1
ID_FLARM = 2

# ADS-B / UAT / ADS-L emitter category (DO-260 numbering, as in the DroneScout aircraft
# message) -> FLARM aircraft type. 12 "ultralight / hang glider / paraglider" can be either
# powered or not: unknown.
_ACFT_TYPE = {1: ACFT_POWERED, 2: ACFT_JET, 3: ACFT_JET, 4: ACFT_JET, 5: ACFT_JET, 6: ACFT_JET,
              7: ACFT_HELICOPTER, 9: ACFT_GLIDER, 10: ACFT_BALLOON, 11: ACFT_SKYDIVER, 14: ACFT_UAV,
              19: ACFT_STATIC, 20: ACFT_STATIC, 21: ACFT_STATIC}

EARTH_RADIUS_M = 6371008.8


def checksum(body):
    cs = 0
    for c in body.encode('ascii'):
        cs ^= c
    return cs


def sentence(body):
    """'PFLAU,...' -> '$PFLAU,...*CS\\r\\n'."""
    return '$%s*%02X\r\n' % (body, checksum(body))


def parse_sentence(line):
    """'$...*CS' -> list of fields (fields[0] = sentence name), None if the checksum is wrong."""
    line = line.strip()
    if not line.startswith('$') or '*' not in line:
        return None
    body, cs = line[1:].rsplit('*', 1)
    try:
        if int(cs, 16) != checksum(body):
            return None
    except ValueError:
        return None
    return body.split(',')


def _known(value):
    return value is not None and not (isinstance(value, float) and math.isnan(value))


def relative_position(own_lat, own_lon, lat, lon):
    """(north, east) in meters of lat/lon seen from the own position (local flat earth, fine for tens of km)."""
    north = math.radians(lat - own_lat) * EARTH_RADIUS_M
    east = math.radians(lon - own_lon) * EARTH_RADIUS_M * math.cos(math.radians((lat + own_lat) / 2.0))
    return north, east


def _nmea_lat_lon(lat, lon):
    def fmt(value, degree_digits):
        a = abs(value)
        d = int(a)
        m = (a - d) * 60.0
        if round(m, 4) >= 60.0:
            d, m = d + 1, 0.0
        return '%0*d%07.4f' % (degree_digits, d, m)
    return fmt(lat, 2), 'N' if lat >= 0 else 'S', fmt(lon, 3), 'E' if lon >= 0 else 'W'


def gprmc(lat, lon, now=None):
    """Own position, standing still (a ground station)."""
    t = datetime.fromtimestamp(time.time() if now is None else now, tz=timezone.utc)
    la, ns, lo, ew = _nmea_lat_lon(lat, lon)
    return sentence('GPRMC,%s,A,%s,%s,%s,%s,0.0,0.0,%s,,,A'
                    % (t.strftime('%H%M%S.') + '%02d' % (t.microsecond // 10000), la, ns, lo, ew, t.strftime('%d%m%y')))


def gpgga(lat, lon, alt_msl_m, now=None, satellites=8, hdop=1.0):
    t = datetime.fromtimestamp(time.time() if now is None else now, tz=timezone.utc)
    la, ns, lo, ew = _nmea_lat_lon(lat, lon)
    return sentence('GPGGA,%s,%s,%s,%s,%s,1,%02d,%.1f,%.1f,M,,M,,'
                    % (t.strftime('%H%M%S.') + '%02d' % (t.microsecond // 10000), la, ns, lo, ew,
                       satellites, hdop, alt_msl_m))


def pflau(rx_count, gps_ok, tx=False):
    """Status / heartbeat, once per second. No alarms: DroneScout gives traffic information,
    not FLARM collision warnings. TX 0: DroneScout doesn't transmit."""
    return sentence('PFLAU,%d,%d,%d,1,0,,0,,,' % (min(rx_count, 99), 1 if tx else 0, 2 if gps_ok else 0))


def pflaa(north_m, east_m, vertical_m, id_type, address, track_deg=None, speed_ms=None, climb_ms=None,
          acft_type=ACFT_UNKNOWN):
    """One traffic target. Unknown track / speed / climb rate are left empty."""
    track = ('%d' % (int(round(track_deg)) % 360)) if _known(track_deg) and _known(speed_ms) else ''
    speed = ('%d' % int(round(speed_ms))) if _known(speed_ms) else ''
    climb = ('%.1f' % max(-32.7, min(32.7, climb_ms))) if _known(climb_ms) else ''
    vertical = ('%d' % int(round(vertical_m))) if _known(vertical_m) else ''
    return sentence('PFLAA,0,%d,%d,%s,%d,%s,%s,,%s,%s,%s'
                    % (int(round(north_m)), int(round(east_m)), vertical, id_type, address.upper(),
                       track, speed, climb, acft_type))


def drone_target(msg, own):
    """PFLAA for a RemoteIDMessage seen from own = (lat, lon, altitude MSL m, height above ground m),
    None without a position.

    Relative vertical: RemoteID height above ground minus the own height above ground when
    the drone sends it (the best vertical value for a drone near a ground station), else
    the pressure altitude, else the geodetic altitude, minus the own altitude MSL."""
    loc = msg.uas.location
    if not loc.has_position:
        return None
    own_lat, own_lon, own_msl, own_agl = own
    north, east = relative_position(own_lat, own_lon, loc.latitude, loc.longitude)
    if _known(loc.height) and loc.height_type == 1:
        vertical = loc.height - (own_agl or 0.0)
    elif _known(loc.altitude_baro):
        vertical = loc.altitude_baro - own_msl
    elif _known(loc.altitude_geo):
        vertical = loc.altitude_geo - own_msl
    else:
        vertical = None
    return pflaa(north, east, vertical, ID_RANDOM, decode.pseudo_icao(decode.drone_id(msg)),
                 loc.direction, loc.speed_horizontal, loc.speed_vertical, ACFT_UAV)


def aircraft_target(msg, own):
    """PFLAA for an AircraftMessage, None without a position."""
    lat, lon = msg.get('latitude'), msg.get('longitude')
    if not (_known(lat) and _known(lon)) or (lat == 0 and lon == 0):
        return None
    own_lat, own_lon, own_msl, _ = own
    north, east = relative_position(own_lat, own_lon, lat, lon)
    alt = msg.get('altitude')
    address_type = msg.get('address type')
    id_type = ID_ICAO if address_type in (0, 2) else ID_FLARM if address_type == 9 else ID_RANDOM
    category = msg.get('category')
    acft = _ACFT_TYPE.get(category, ACFT_UNKNOWN) if isinstance(category, int) else ACFT_UNKNOWN
    return pflaa(north, east, (alt - own_msl) if _known(alt) else None, id_type,
                 (msg.get('address') or '000000')[-6:].rjust(6, '0'),
                 msg.get('track'), msg.get('speed'), msg.get('vertical rate'), acft)

# (c) Bluemark Innovations BV
# MIT license
"""GDL90 messages from decoded DroneScout messages.

GDL90 (Garmin, "GDL 90 Data Interface Specification", 560-1058-00 Rev A) is what
ADS-B receivers such as Stratux send to cockpit apps (EFBs: ForeFlight, Garmin
Pilot, SkyDemon, ...) over UDP port 4000. Pure functions: message in, bytes out.

Frame: 0x7E | message ID | data | CRC-16-CCITT (LSB first) | 0x7E, with 0x7D / 0x7E
in between escaped as 0x7D, byte ^ 0x20.
"""

import math
import struct
import time
from datetime import datetime, timezone

from modules import decode

FT = 0.3048
KT = 1852.0 / 3600.0           # m/s per knot
FPM = FT / 60.0                # m/s per ft/min

MSG_HEARTBEAT = 0
MSG_OWNSHIP = 10
MSG_OWNSHIP_GEO_ALT = 11
MSG_TRAFFIC = 20
MSG_FOREFLIGHT = 0x65

ADDR_ADSB_ICAO = 0
ADDR_ADSB_SELF_ASSIGNED = 1
ADDR_TISB_ICAO = 2
ADDR_TISB_TRACK_FILE = 3
ADDR_SURFACE_VEHICLE = 4
ADDR_GROUND_BEACON = 5

EMITTER_UAV = 14

# DroneScout "address type" -> GDL90 address type
_ADDRESS_TYPE = {0: ADDR_ADSB_ICAO, 2: ADDR_TISB_ICAO, 3: ADDR_TISB_TRACK_FILE,
                 4: ADDR_SURFACE_VEHICLE, 5: ADDR_GROUND_BEACON}


def _crc_table():
    table = []
    for i in range(256):
        crc = i << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
        table.append(crc & 0xFFFF)
    return table


CRC_TABLE = _crc_table()


def crc16(data):
    """GDL90 frame check sequence (CRC-16-CCITT, initial value 0), as in the specification."""
    crc = 0
    for b in data:
        crc = (CRC_TABLE[crc >> 8] ^ (crc << 8) ^ b) & 0xFFFF
    return crc


def frame(message):
    """Add the CRC, escape and add the flag bytes."""
    crc = crc16(message)
    body = bytes(message) + bytes((crc & 0xFF, crc >> 8))
    out = bytearray(b'\x7e')
    for b in body:
        if b in (0x7D, 0x7E):
            out += bytes((0x7D, b ^ 0x20))
        else:
            out.append(b)
    out.append(0x7E)
    return bytes(out)


def unframe(data):
    """Messages in a datagram / stream (bytes) -> [message without CRC]; frames with a bad CRC are skipped."""
    out = []
    for chunk in bytes(data).split(b'\x7e'):
        if not chunk:
            continue
        raw = bytearray()
        escape = False
        for b in chunk:
            if escape:
                raw.append(b ^ 0x20)
                escape = False
            elif b == 0x7D:
                escape = True
            else:
                raw.append(b)
        if len(raw) < 3:
            continue
        msg, crc = bytes(raw[:-2]), raw[-2] | (raw[-1] << 8)
        if crc16(msg) == crc:
            out.append(msg)
    return out


def _known(value):
    return value is not None and not (isinstance(value, float) and math.isnan(value))


def _lat_lon(value):
    """24-bit signed, 180 / 2^23 degrees."""
    v = int(round(value / (180.0 / 2 ** 23)))
    v = max(-0x800000, min(0x7FFFFF, v))
    return (v & 0xFFFFFF).to_bytes(3, 'big')


def _seconds_since_midnight(epoch_s):
    t = datetime.fromtimestamp(epoch_s, tz=timezone.utc)
    return t.hour * 3600 + t.minute * 60 + t.second


def heartbeat(now=None, gps_valid=False, message_count=0):
    """Heartbeat (ID 0), once per second. UTC OK: the gateway's own clock."""
    now = time.time() if now is None else now
    ts = _seconds_since_midnight(now)
    status1 = (0x80 if gps_valid else 0) | 0x01            # GPS position valid, UAT initialized
    status2 = ((ts >> 16) & 1) << 7 | 0x01                  # timestamp bit 16, UTC OK
    counts = min(message_count, 0x3FF)                      # uplink (5 bits) = 0, basic + long (10 bits)
    return bytes((MSG_HEARTBEAT, status1, status2)) + struct.pack('<H', ts & 0xFFFF) + struct.pack('>H', counts)


def foreflight_id(serial=None, name='DroneSct', long_name='DroneScout', geo_alt_msl=False):
    """ForeFlight ID message (0x65, sub-ID 0): device name shown in the EFB.
    Capabilities bit 0: geometric altitude datum of message 11 (0 = WGS-84 ellipsoid, 1 = MSL)."""
    serial_bytes = (serial.to_bytes(8, 'big') if serial is not None else b'\xff' * 8)
    return (bytes((MSG_FOREFLIGHT, 0, 1)) + serial_bytes + name.encode('ascii', 'replace')[:8].ljust(8)
            + long_name.encode('ascii', 'replace')[:16].ljust(16) + struct.pack('>I', 1 if geo_alt_msl else 0))


def _report(msg_id, address_type, address, lat, lon, pressure_alt_m, airborne, nic, nacp, speed_ms,
            vertical_ms, track_deg, emitter, callsign, emergency=0, alert=0):
    """Ownship (10) / traffic (20) report, 28 bytes."""
    if _known(pressure_alt_m):
        alt = int(round((pressure_alt_m / FT + 1000) / 25.0))
        alt = max(0, min(0xFFE, alt))
    else:
        alt = 0xFFF
    track_known = _known(track_deg) and _known(speed_ms)
    misc = (0x08 if airborne else 0) | (0x01 if track_known else 0)       # airborne, true track; report updated
    if _known(speed_ms):
        hvel = max(0, min(0xFFE, int(round(speed_ms / KT))))
    else:
        hvel = 0xFFF
    if _known(vertical_ms):
        vvel = max(-0x1FE, min(0x1FE, int(round(vertical_ms / FPM / 64.0)))) & 0xFFF
    else:
        vvel = 0x800
    trk = int(round((track_deg % 360.0) / (360.0 / 256))) % 256 if track_known else 0
    cs = ''.join(c for c in (callsign or '').upper() if c.isalnum() or c == ' ')[:8].ljust(8)
    return (bytes((msg_id, (alert & 0x0F) << 4 | (address_type & 0x0F))) + (address & 0xFFFFFF).to_bytes(3, 'big')
            + _lat_lon(lat) + _lat_lon(lon)
            + bytes((alt >> 4, (alt & 0x0F) << 4 | misc, (nic & 0x0F) << 4 | (nacp & 0x0F)))
            + bytes((hvel >> 4, (hvel & 0x0F) << 4 | (vvel >> 8), vvel & 0xFF, trk, emitter & 0xFF))
            + cs.encode('ascii') + bytes(((emergency & 0x0F) << 4,)))


def drone_address(msg):
    return int(decode.pseudo_icao(decode.drone_id(msg)), 16)


def drone_traffic(msg, nic=None):
    """Traffic report for a RemoteIDMessage, None without a position.

    Altitude: GDL90 traffic altitude is pressure altitude. RemoteID sends one (altitude_baro,
    pressure altitude referenced to 29.92 inHg); without it the geodetic altitude is used.
    NACp: the RemoteID horizontal accuracy codes are the ADS-B NACp values. NIC isn't sent by
    RemoteID: by default the same value as NACp (an EFB may hide traffic with NIC 0)."""
    loc = msg.uas.location
    if not loc.has_position:
        return None
    alt = loc.altitude_baro if _known(loc.altitude_baro) else loc.altitude_geo
    nacp = loc.horiz_accuracy if 0 <= loc.horiz_accuracy <= 11 else 0
    airborne = loc.status != 1                                     # 1 = on the ground
    return _report(MSG_TRAFFIC, ADDR_ADSB_SELF_ASSIGNED, drone_address(msg), loc.latitude, loc.longitude, alt,
                   airborne, nacp if nic is None else nic, nacp, loc.speed_horizontal, loc.speed_vertical,
                   loc.direction, EMITTER_UAV, decode.short_callsign(decode.drone_id(msg)),
                   emergency=1 if loc.status == 3 else 0)


def aircraft_traffic(msg, nic=0, nacp=0):
    """Traffic report for an AircraftMessage (ADS-B, UAT, ADS-L), None without a position.
    The sensor doesn't pass on NIC / NACp: 0 (unknown) unless set."""
    lat, lon = msg.get('latitude'), msg.get('longitude')
    if not (_known(lat) and _known(lon)) or (lat == 0 and lon == 0):
        return None
    try:
        address = int(msg.get('address') or '0', 16)
    except ValueError:
        return None
    address_type = _ADDRESS_TYPE.get(msg.get('address type'), ADDR_ADSB_SELF_ASSIGNED)
    category = msg.get('category')
    emitter = int(category) if isinstance(category, int) and 0 <= category <= 39 else 0
    emergency = msg.get('emergency status')
    return _report(MSG_TRAFFIC, address_type, address, lat, lon, msg.get('altitude'),
                   msg.get('flight state') != 0, nic, nacp, msg.get('speed'), msg.get('vertical rate'),
                   msg.get('track'), emitter, (msg.get('flight') or '').strip(),
                   emergency=1 if emergency == 1 else 0)


def ownship(lat, lon, alt_msl_m, name='DSCOUT'):
    """Ownship report (10) for a fixed position (a ground display), on the ground."""
    return _report(MSG_OWNSHIP, ADDR_ADSB_SELF_ASSIGNED, 0xFFFFFE, lat, lon, alt_msl_m, False, 8, 9,
                   0.0, 0.0, None, 0, name)


def ownship_geo_altitude(alt_m):
    """Ownship geometric altitude (11): 5 ft resolution; vertical metrics unknown (0x7FFF)."""
    return bytes((MSG_OWNSHIP_GEO_ALT,)) + struct.pack('>h', int(round(alt_m / FT / 5.0))) + b'\x7f\xff'


# --- decoding (tests, tools) -----------------------------------------------------------------

def _s(value, bits):
    return value - (1 << bits) if value & (1 << (bits - 1)) else value


def parse_report(msg):
    """Ownship / traffic report -> dict (values in GDL90 units: ft, kt, fpm)."""
    alt = (msg[11] << 4) | (msg[12] >> 4)
    hvel = (msg[14] << 4) | (msg[15] >> 4)
    vvel = ((msg[15] & 0x0F) << 8) | msg[16]
    return {
        'id': msg[0], 'alert': msg[1] >> 4, 'address_type': msg[1] & 0x0F,
        'address': int.from_bytes(msg[2:5], 'big'),
        'lat': _s(int.from_bytes(msg[5:8], 'big'), 24) * 180.0 / 2 ** 23,
        'lon': _s(int.from_bytes(msg[8:11], 'big'), 24) * 180.0 / 2 ** 23,
        'alt_ft': None if alt == 0xFFF else alt * 25 - 1000,
        'misc': msg[12] & 0x0F, 'nic': msg[13] >> 4, 'nacp': msg[13] & 0x0F,
        'speed_kt': None if hvel == 0xFFF else hvel,
        'vs_fpm': None if vvel == 0x800 else _s(vvel, 12) * 64,
        'track': msg[17] * 360.0 / 256, 'emitter': msg[18],
        'callsign': msg[19:27].decode('ascii').rstrip(), 'emergency': msg[27] >> 4,
    }

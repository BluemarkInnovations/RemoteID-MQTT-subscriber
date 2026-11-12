# (c) Bluemark Innovations BV
# MIT license
"""ASTERIX records from decoded DroneScout messages.

Pure functions: decoded message in, ASTERIX record (or datablock bytes) out.
The UDP side (batching, heartbeats, destinations) is in asterix_export.py.

Categories and editions (EUROCONTROL category list of 22 October 2025):

    CAT021 2.7   ADS-B target reports: ADS-B 1090 and UAT aircraft
    CAT023 1.3   CNS/ATM ground station and service status: every DroneScout sensor
    CAT129 1.2   UAS identification reports: RemoteID drones
    CAT062 1.21  system track data: drones (and optionally aircraft) as one air picture
    CAT065 1.6   SDPS service status: the gateway as track source for CAT062

CAT021/023/062/065 are encoded with libasterix (asterix-libs); CAT129 has no
library support, so it is encoded here (cat129_record) from the EUROCONTROL
specification, Part 29, edition 1.2 (12 June 2019).

Only values the sensor really decodes are sent. Items the categories make
mandatory but DroneScout can't fill are sent with the value the specification
defines as "unknown" (e.g. I021/090 quality indicators = 0).
"""

import math
import time
from datetime import datetime, timezone

from asterix.base import Bits, RawDatablock
import asterix.generated as gen

from modules import decode

CAT021 = gen.Cat_021_2_7
CAT023 = gen.Cat_023_1_3
CAT062 = gen.Cat_062_1_21
CAT065 = gen.Cat_065_1_6

FT = 0.3048
NM = 1852.0

# DroneScout "category" (DO-260 emitter category sets A/B/C, 0..23) -> I021/020 / I062/380 EMC
_EMITTER_CATEGORY = {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 10,
                     9: 11, 10: 12, 11: 16, 12: 15, 14: 13, 15: 14,
                     17: 20, 18: 21, 19: 22, 20: 23, 21: 24}

# RemoteID horizontal accuracy codes -> 95 % bound in meters
_HORIZ_ACCURACY_M = {1: 18520.0, 2: 7408.0, 3: 3704.0, 4: 1852.0, 5: 926.0, 6: 555.6,
                     7: 185.2, 8: 92.6, 9: 30.0, 10: 10.0, 11: 3.0, 12: 1.0}
# circular normal distribution: CEP50 = 1.1774 sigma, R95 = 2.4477 sigma
_R95_TO_CEP50 = 1.1774 / 2.4477


def _known(value):
    return value is not None and not (isinstance(value, float) and math.isnan(value))


def time_of_day(epoch_s):
    """Seconds since midnight UTC (the ASTERIX time items wrap at midnight)."""
    t = datetime.fromtimestamp(epoch_s, tz=timezone.utc)
    return t.hour * 3600 + t.minute * 60 + t.second + t.microsecond / 1e6


def _tod_quantity(epoch_s):
    tod = time_of_day(epoch_s)
    return (min(tod, 86400 - 1.0 / 128), 's')


def _sensor_time(timestamp_ms, now):
    """Sensor timestamp (ms since epoch) in s; the gateway time if it is missing."""
    if timestamp_ms:
        return timestamp_ms / 1000.0
    return now


def emitter_category(category):
    try:
        return _EMITTER_CATEGORY.get(int(category), 0)
    except (TypeError, ValueError):
        return 0


def _callsign(text):
    """I021/170 / I062/380 ID: ICAO 6-bit characters, A-Z 0-9 space, at most 8."""
    text = ''.join(c for c in (text or '').upper() if c.isalnum() or c == ' ').strip()
    return text[:8] if text else None


# --- datablocks ----------------------------------------------------------------------

def datablocks(category_class, records, max_bytes=1400):
    """Records -> list of datablock bytes, each at most max_bytes (one UDP datagram)."""
    out, batch, size = [], [], 3
    for r in records:
        n = len(r.unparse().to_bytes())
        if batch and size + n > max_bytes:
            out.append(category_class.create(batch).unparse().to_bytes())
            batch, size = [], 3
        batch.append(r)
        size += n
    if batch:
        out.append(category_class.create(batch).unparse().to_bytes())
    return out


def raw_datablock(category, records, max_bytes=1400):
    """Datablocks for hand-encoded records (CAT129): CAT, LEN (2 bytes), records."""
    out, batch = [], b''
    for r in records:
        if batch and 3 + len(batch) + len(r) > max_bytes:
            out.append(bytes([category]) + (3 + len(batch)).to_bytes(2, 'big') + batch)
            batch = b''
        batch += r
    if batch:
        out.append(bytes([category]) + (3 + len(batch)).to_bytes(2, 'big') + batch)
    return out


# --- CAT021: ADS-B / UAT aircraft --------------------------------------------------------

def cat021_record(msg, sac, sic, track_number=None, service_id=None, now=None):
    """CAT021 record for an ADS-B 1090 or UAT AircraftMessage, None for other
    technologies (ADS-L / FLARM / OGN aren't ADS-B) or without a position."""
    now = time.time() if now is None else now
    if msg.get('type') not in ('ADSB', 'UAT'):
        return None
    try:
        lat, lon = float(msg.get('latitude')), float(msg.get('longitude'))
        address = int(str(msg.get('address')), 16)
    except (TypeError, ValueError):
        return None
    if (lat == 0.0 and lon == 0.0) or not (-90 <= lat <= 90 and -180 <= lon < 180) or not 0 <= address < 1 << 24:
        return None

    atp = 0 if msg.get('address type') in (0, 2) else 3       # ICAO address, else "anonymous"
    gbs = 1 if msg.get('flight state') == 0 else 0
    t = _sensor_time(msg.get('timestamp'), now)
    items = {
        '010': (('SAC', sac), ('SIC', sic)),
        # ARC 2: altitude reporting capability unknown; extension: GBS, SIM 0, TST 0, SAA 0, CL 2 (no info)
        '040': ((('ATP', atp), ('ARC', 2), ('RC', 0), ('RAB', 0), None),
                (('DCR', 0), ('GBS', gbs), ('SIM', 0), ('TST', 0), ('SAA', 0), ('CL', 2), None))
               if gbs else ((('ATP', atp), ('ARC', 2), ('RC', 0), ('RAB', 0), None),),
        '073': _tod_quantity(t),                 # time of message reception for position
        '131': (('LAT', (lat, '°')), ('LON', (lon, '°'))),
        '080': address,
        # mandatory; the sensor doesn't decode NACp/NIC/NACv yet: 0 = unknown
        '090': ((('NUCRNACV', 0), ('NUCPNIC', 0), None),),
        '077': _tod_quantity(now),               # time of ASTERIX report transmission
    }
    if track_number is not None:
        items['161'] = (0, ('TRNUM', track_number & 0xFFF))
    if service_id is not None:
        items['015'] = service_id

    alt = msg.get('altitude')
    alt_type = msg.get('altitude type')
    if _known(alt):
        if alt_type == 1:                        # barometric: flight level
            fl = alt / FT / 100.0
            if -15 <= fl <= 1500:
                items['145'] = (fl, 'FL')
        elif alt_type == 2:                      # GNSS height above the WGS-84 ellipsoid
            ft = alt / FT
            if -1500 <= ft <= 150000:
                items['140'] = (ft, 'ft')

    speed, track = msg.get('speed'), msg.get('track')
    if _known(speed) and _known(track) and not (speed == 0 and track == 0):
        gs = speed / NM
        if 0 <= gs < 2:
            items['160'] = (('RE', 0), ('GS', (gs, 'NM/s')), ('TA', (float(track) % 360.0, '°')))
    vr = msg.get('vertical rate')
    if _known(vr) and not (vr == 0 and speed == 0 and track == 0):
        fpm = max(min(vr / FT * 60.0, 81900.0), -81900.0)
        key, name = ('157', 'GVR') if alt_type == 2 else ('155', 'BVR')
        items[key] = (('RE', 0), (name, (fpm, 'ft/min')))
    callsign = _callsign(msg.get('flight'))
    if callsign:
        items['170'] = callsign
    if msg.get('category') is not None:
        items['020'] = emitter_category(msg.get('category'))
    rssi = msg.get('RSSI')
    if _known(rssi):
        items['132'] = (int(max(min(round(rssi), 127), -128)), 'dBm')
    return CAT021.cv_record.create(items)


# --- CAT023: the sensors as ground stations ----------------------------------------------

SERVICE_1090 = 1     # service identification of ADS-B 1090 ES (also I021/015)
SERVICE_UAT = 2      # service identification of ADS-B UAT

_STYP = {SERVICE_1090: 2, SERVICE_UAT: 3}               # I023/015 type: ES, UAT
_SDR_NAME = {SERVICE_1090: '1090', SERVICE_UAT: '978'}


def _service_state(status_json, service_id):
    """I023/110 STAT from the sensor status: 4 normal, 3 degraded, 1 failed, 0 unknown."""
    if not status_json:
        return 0
    sdr = status_json.get('SDR receivers')
    if isinstance(sdr, dict) and _SDR_NAME[service_id] in sdr:
        state = sdr[_SDR_NAME[service_id]]
        return {'ok': 4, 'quiet': 4, 'stalled': 1, 'missing': 1}.get(state, 3)
    return {'normal': 4, 'ok': 4, 'degraded': 3}.get(status_json.get('status'), 0)


def cat023_records(sac, sic, status_json, period_s, report_period_s, range_nm=None,
                   services=(SERVICE_1090, SERVICE_UAT), now=None):
    """Ground station status report + one service status report per service."""
    now = time.time() if now is None else now
    gssp = int(max(1, min(127, round(period_s))))
    nogo = 0 if status_json and status_json.get('status') in ('normal', 'ok', 'degraded') else 1
    records = [CAT023.cv_record.create({
        '000': 1,
        '010': (('SAC', sac), ('SIC', sic)),
        '070': _tod_quantity(now),
        '100': ((('NOGO', nogo), ('ODP', 0), ('OXT', 0), ('MSC', 0), ('TSV', 0), ('SPO', 0), ('RN', 0), None),
                (('GSSP', (gssp, 's')), None)),
    })]
    for sid in services:
        items = {
            '000': 2,
            '010': (('SAC', sac), ('SIC', sic)),
            '015': (('SID', sid), ('STYP', _STYP[sid])),
            '070': _tod_quantity(now),
            '101': ((('RP', (min(max(report_period_s, 0.5), 127.5), 's')), ('SC', 0), 0, None),
                    (('SSRP', (gssp, 's')), None)),
            '110': ((0, ('STAT', _service_state(status_json, sid)), None),),
        }
        if range_nm:
            items['200'] = (int(min(max(range_nm, 0), 255)), 'NM')
        records.append(CAT023.cv_record.create(items))
    return records


# --- CAT129: RemoteID drones ---------------------------------------------------------------

def _s(value, bits, lsb):
    """Two's complement of round(value / lsb) in the given number of bits (saturated)."""
    n = int(round(value / lsb))
    lim = 1 << (bits - 1)
    n = max(min(n, lim - 1), -lim)
    return n & ((1 << bits) - 1)


def cat129_record(msg, sac, sic, country='ZZ', destination=None, now=None, special_purpose=True):
    """CAT129 1.2 record for a RemoteIDMessage (bytes), None without position or altitude.

    - I129/040 (12 octets) can't hold a full ANSI/CTA-2063 serial (up to 20 characters):
      it carries the last 12 characters; the full ID goes in the SP field as "ID=<id>".
    - I129/050 (country of the manufacturer ID) is mandatory but unknown for RemoteID:
      configurable, default "ZZ" (ISO 3166 user-assigned, used for "unknown").
    - I129/090 AMSL: barometric (pressure) altitude, else geodetic altitude as an
      approximation. I129/100 AGL: the RemoteID height when its reference is the ground.
    - I129/070: the time of transmission, as the specification defines it.
    - I129/120 (risk levels) is left out: "ARC = 0" means ARC-1, there is no "unknown".
    """
    now = time.time() if now is None else now
    loc = msg.uas.location
    if not loc.has_position:
        return None
    if not (-90 <= loc.latitude <= 90 and -180 <= loc.longitude < 180):
        return None
    amsl = loc.altitude_baro if _known(loc.altitude_baro) else loc.altitude_geo
    agl = loc.height if (_known(loc.height) and loc.height_type == 1) else None
    if not _known(amsl) and agl is None:
        return None

    drone = decode.drone_id(msg)
    items = {}                                         # FRN -> bytes
    items[1] = bytes([sac & 0xFF, sic & 0xFF])
    if destination is not None:
        items[2] = bytes([destination[0] & 0xFF, destination[1] & 0xFF])
    serial = drone.encode('ascii', 'replace')[-12:]
    items[5] = serial.rjust(12, b' ')                  # right-aligned, space padded
    items[6] = (country or 'ZZ').upper().encode('ascii', 'replace')[:2].ljust(2, b'Z')
    items[7] = (int(time_of_day(now) * 128) % (1 << 24)).to_bytes(3, 'big')
    items[8] = (_s(loc.latitude, 32, 180.0 / 2 ** 30).to_bytes(4, 'big')
                + _s(loc.longitude, 32, 180.0 / 2 ** 30).to_bytes(4, 'big'))
    if _known(amsl):
        items[9] = _s(amsl, 24, 0.1).to_bytes(3, 'big')
    if agl is not None:
        items[10] = _s(agl, 24, 0.1).to_bytes(3, 'big')
    acc = _HORIZ_ACCURACY_M.get(loc.horiz_accuracy)
    if acc is not None:
        cep = int(round(acc * _R95_TO_CEP50))
        items[11] = (cep if 1 <= cep <= 255 else 0).to_bytes(2, 'big')   # 0: unknown or > 255 m
    if special_purpose:
        sp = ('ID=%s' % drone).encode('ascii', 'replace')[:254]
        items[13] = bytes([len(sp) + 1]) + sp           # SP: length octet includes itself
    if _known(loc.speed_horizontal) and _known(loc.direction):
        d = math.radians(loc.direction)
        hvx = _s(loc.speed_horizontal * math.sin(d), 20, 0.01)
        hvy = _s(loc.speed_horizontal * math.cos(d), 20, 0.01)
        items[14] = ((hvx << 20) | hvy).to_bytes(5, 'big')
    if _known(loc.speed_vertical):
        items[15] = _s(loc.speed_vertical, 20, 0.01).to_bytes(3, 'big')   # 4 spare bits = 0

    # FSPEC: 7 FRNs per octet, bit 1 = FX
    last = max(items)
    n_octets = (last - 1) // 7 + 1
    fspec = bytearray(n_octets)
    for frn in items:
        octet, pos = (frn - 1) // 7, (frn - 1) % 7
        fspec[octet] |= 0x80 >> pos
    for i in range(n_octets - 1):
        fspec[i] |= 0x01
    return bytes(fspec) + b''.join(items[frn] for frn in sorted(items))


def parse_cat129(data):
    """Decode CAT129 records (the payload of a datablock) into dicts. For tests and
    tools/asterix_dump.py: no common decoder knows CAT129."""
    records, i = [], 0
    sizes = {1: 2, 2: 2, 3: 3, 4: 3, 5: 12, 6: 2, 7: 3, 8: 8, 9: 3, 10: 3, 11: 2, 12: 1, 14: 5, 15: 3}

    def signed(b, bits):
        v = int.from_bytes(b, 'big') & ((1 << bits) - 1)
        return v - (1 << bits) if v & (1 << (bits - 1)) else v

    while i < len(data):
        frns, octet = [], 0
        while True:
            b = data[i]
            i += 1
            for pos in range(7):
                if b & (0x80 >> pos):
                    frns.append(octet * 7 + pos + 1)
            octet += 1
            if not b & 1:
                break
        rec = {}
        for frn in frns:
            if frn == 13:
                n = data[i]
                rec['SP'] = data[i + 1:i + n]
                i += n
                continue
            if frn not in sizes:
                raise ValueError('CAT129: unknown FRN %d' % frn)
            v = data[i:i + sizes[frn]]
            i += sizes[frn]
            if frn == 1:
                rec['SAC'], rec['SIC'] = v[0], v[1]
            elif frn == 2:
                rec['DAC'], rec['DIC'] = v[0], v[1]
            elif frn in (3, 4):
                rec[{3: 'manufacturer', 4: 'model'}[frn]] = v.decode('ascii', 'replace')
            elif frn == 5:
                rec['serial'] = v.decode('ascii', 'replace').strip()
            elif frn == 6:
                rec['country'] = v.decode('ascii', 'replace')
            elif frn == 7:
                rec['tod'] = int.from_bytes(v, 'big') / 128.0
            elif frn == 8:
                rec['lat'] = signed(v[:4], 32) * 180.0 / 2 ** 30
                rec['lon'] = signed(v[4:], 32) * 180.0 / 2 ** 30
            elif frn == 9:
                rec['amsl'] = signed(v, 24) * 0.1
            elif frn == 10:
                rec['agl'] = signed(v, 24) * 0.1
            elif frn == 11:
                rec['gnss_accuracy'] = int.from_bytes(v, 'big')
            elif frn == 12:
                rec['risk'] = v[0]
            elif frn == 14:
                x = int.from_bytes(v, 'big')
                rec['vx'] = signed((x >> 20).to_bytes(3, 'big'), 20) * 0.01
                rec['vy'] = signed((x & 0xFFFFF).to_bytes(3, 'big'), 20) * 0.01
            elif frn == 15:
                rec['vv'] = signed(v, 20) * 0.01
        records.append(rec)
    return records


# --- CAT062: one air picture with system tracks ----------------------------------------------

def _cat062_status(first, last, geometric=True, source_gnss=True):
    group1 = (('MON', 1), ('SPI', 0), ('MRH', 1 if geometric else 0), ('SRC', 1 if source_gnss else 0),
              ('CNF', 0), None)
    if not (first or last):
        return (group1,)
    return (group1, (('SIM', 0), ('TSE', 1 if last else 0), ('TSB', 1 if first else 0), ('FPC', 0),
                     ('AFF', 0), ('STP', 0), ('KOS', 0), None))


def cat062_drone_record(msg, sac, sic, track_number, service_id=None, first=False, last=False, now=None):
    """CAT062 system track for a RemoteID drone, None without position."""
    now = time.time() if now is None else now
    loc = msg.uas.location
    if not loc.has_position or not (-90 <= loc.latitude <= 90 and -180 <= loc.longitude < 180):
        return None
    items = {
        '010': (('SAC', sac), ('SIC', sic)),
        '040': track_number & 0xFFFF,
        '070': _tod_quantity(_sensor_time(msg.timestamp_ms, now)),
        '080': _cat062_status(first, last, geometric=True, source_gnss=True),
        '105': (('LAT', (loc.latitude, '°')), ('LON', (loc.longitude, '°'))),
        '380': {'EMC': 13},                      # unmanned aerial vehicle
    }
    if service_id is not None:
        items['015'] = service_id
    if _known(loc.altitude_geo) and -1500 <= loc.altitude_geo / FT <= 150000:
        items['130'] = (loc.altitude_geo / FT, 'ft')        # geodetic = WGS-84 ellipsoid
    if _known(loc.speed_horizontal) and _known(loc.direction):
        d = math.radians(loc.direction)
        items['185'] = (('VX', (loc.speed_horizontal * math.sin(d), 'm/s')),
                        ('VY', (loc.speed_horizontal * math.cos(d), 'm/s')))
    if _known(loc.speed_vertical):
        items['220'] = (loc.speed_vertical / FT * 60.0, 'ft/min')
    return CAT062.cv_record.create(items)


def cat062_aircraft_record(msg, sac, sic, track_number, service_id=None, first=False, last=False, now=None):
    """CAT062 system track for an aircraft (any technology), None without position."""
    now = time.time() if now is None else now
    try:
        lat, lon = float(msg.get('latitude')), float(msg.get('longitude'))
    except (TypeError, ValueError):
        return None
    if (lat == 0.0 and lon == 0.0) or not (-90 <= lat <= 90 and -180 <= lon < 180):
        return None
    alt, alt_type = msg.get('altitude'), msg.get('altitude type')
    items = {
        '010': (('SAC', sac), ('SIC', sic)),
        '040': track_number & 0xFFFF,
        '070': _tod_quantity(_sensor_time(msg.get('timestamp'), now)),
        '080': _cat062_status(first, last, geometric=alt_type == 2, source_gnss=alt_type == 2),
        '105': (('LAT', (lat, '°')), ('LON', (lon, '°'))),
    }
    if service_id is not None:
        items['015'] = service_id
    if _known(alt):
        if alt_type == 1 and -15 <= alt / FT / 100.0 <= 1500:
            items['136'] = (alt / FT / 100.0, 'FL')
        elif alt_type == 2 and -1500 <= alt / FT <= 150000:
            items['130'] = (alt / FT, 'ft')
    speed, track = msg.get('speed'), msg.get('track')
    if _known(speed) and _known(track) and not (speed == 0 and track == 0):
        d = math.radians(track)
        items['185'] = (('VX', (speed * math.sin(d), 'm/s')), ('VY', (speed * math.cos(d), 'm/s')))
    if _known(msg.get('vertical rate')) and not (speed == 0 and track == 0 and msg.get('vertical rate') == 0):
        items['220'] = (msg.get('vertical rate') / FT * 60.0, 'ft/min')
    adr = {}
    try:
        address = int(str(msg.get('address')), 16)
        if msg.get('type') in ('ADSB', 'UAT') and msg.get('address type') in (0, 2) and address < 1 << 24:
            adr['ADR'] = address
    except (TypeError, ValueError):
        pass
    callsign = _callsign(msg.get('flight'))
    if callsign:
        adr['ID'] = callsign
    if msg.get('category') is not None:
        adr['EMC'] = emitter_category(msg.get('category'))
    if adr:
        items['380'] = adr
    return CAT062.cv_record.create(items)


# --- CAT065: the gateway as SDPS ----------------------------------------------------------

def cat065_record(sac, sic, service_id, operational=True, now=None):
    now = time.time() if now is None else now
    return CAT065.cv_record.create({
        '000': 1,                                  # SDPS status
        '010': (('SAC', sac), ('SIC', sic)),
        '015': service_id,
        '030': _tod_quantity(now),
        '040': (('NOGO', 0 if operational else 1), ('OVL', 0), ('TSV', 0), ('PSS', 0), ('STTN', 0), 0),
    })


# --- decoding (tests, tools) ------------------------------------------------------------------

_DECODERS = {21: CAT021, 23: CAT023, 62: CAT062, 65: CAT065}


def parse_datagram(data):
    """bytes -> list of (category, [records]) with libasterix records, or dicts for CAT129."""
    out = []
    blocks = RawDatablock.parse(Bits.from_bytes(data))
    if isinstance(blocks, ValueError):
        raise blocks
    for db in blocks:
        cat = db.get_category()
        raw = db.unparse().to_bytes()[3:]
        if cat == 129:
            out.append((cat, parse_cat129(raw)))
        elif cat in _DECODERS:
            recs = _DECODERS[cat].cv_uap.parse(db.get_raw_records())
            if isinstance(recs, ValueError):
                raise recs
            out.append((cat, recs))
        else:
            out.append((cat, []))
    return out

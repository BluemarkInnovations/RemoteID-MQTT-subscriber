#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Replay recorded CSV logs through the ASTERIX encoder and compare what comes out with the CSV.

Every RemoteID / aircraft row is turned back into a DroneScout message (modules/replay.py),
encoded as CAT021 (ADS-B / UAT), CAT129 and CAT062 (drones, aircraft), decoded again
(libasterix; CAT129 with our decoder) and compared field by field with the CSV values,
within the resolution of each ASTERIX item. No network, no Docker.

    python3 integrations/asterix/tools/replay_check.py logs/*_remoteID_log.csv logs/*_aircraft_log.csv
"""

import argparse
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from modules import decode, replay                            # noqa: E402
from integrations.asterix import asterix_map as am            # noqa: E402

FT = am.FT
NM = am.NM


def _q(rec, item, sub=None, unit=None):
    i = rec.get_item(item)
    if i is None:
        return None
    v = i.variation
    if sub:
        s = v.get_item(sub)
        if s is None:
            return None
        v = s.variation
    return v.content.as_quantity(unit) if unit else v.content.as_uint()


def _str(rec, item, sub=None):
    i = rec.get_item(item)
    if i is None:
        return None
    v = i.variation
    if sub:
        s = v.get_item(sub)
        if s is None:
            return None
        v = s.variation
    return v.content.as_string().strip()


def _known(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and not math.isnan(v)


class Checker:
    def __init__(self):
        self.records = {}          # category -> count
        self.skipped = {}          # reason -> count
        self.checked = {}          # (category, field) -> count
        self.bad = {}              # (category, field) -> count
        self.max_err = {}          # (category, field) -> largest |error|
        self.examples = []

    def skip(self, why):
        self.skipped[why] = self.skipped.get(why, 0) + 1

    def check(self, cat, field, got, want, tol=0.0, row=None):
        """got: from the decoded ASTERIX; want: from the CSV."""
        key = (cat, field)
        self.checked[key] = self.checked.get(key, 0) + 1
        if isinstance(want, float) or isinstance(got, float):
            err = abs((got if got is not None else math.inf) - want)
            self.max_err[key] = max(self.max_err.get(key, 0.0), err)
            ok = err <= tol
        else:
            ok = got == want
        if not ok:
            self.bad[key] = self.bad.get(key, 0) + 1
            if len(self.examples) < 20:
                self.examples.append('CAT%03d %s: ASTERIX %r, CSV %r (%s)' % (cat, field, got, want, row or ''))

    def decode_one(self, category_class, record):
        [(cat, recs)] = am.parse_datagram(am.datablocks(category_class, [record])[0])
        self.records[cat] = self.records.get(cat, 0) + 1
        return recs[0]

    # --- aircraft ---

    def aircraft(self, msg):
        row = '%s %s' % (msg.get('type'), msg.get('address'))
        alt, alt_type = msg.get('altitude'), msg.get('altitude type')
        speed, track = msg.get('speed'), msg.get('track')
        r = am.cat021_record(msg, 0, 1, track_number=1)
        if r is None:
            self.skip('CAT021: not ADS-B / UAT, or no position')
        else:
            rec = self.decode_one(am.CAT021, r)
            self.check(21, 'address', _q(rec, '080'), int(msg.get('address'), 16), row=row)
            self.check(21, 'latitude', _q(rec, '131', 'LAT', '°'), msg.get('latitude'), 180 / 2 ** 30, row)
            self.check(21, 'longitude', _q(rec, '131', 'LON', '°'), msg.get('longitude'), 180 / 2 ** 30, row)
            if _known(alt) and alt_type == 1:
                self.check(21, 'flight level', _q(rec, '145', None, 'FL'), alt / FT / 100, 0.125 + 1e-9, row)
            elif _known(alt) and alt_type == 2:
                self.check(21, 'GNSS height ft', _q(rec, '140', None, 'ft'), alt / FT, 3.125 + 1e-9, row)
            callsign = am._callsign(msg.get('flight'))
            if callsign:
                self.check(21, 'callsign', _str(rec, '170'), callsign, row=row)
            if _known(speed) and _known(track) and not (speed == 0 and track == 0) and speed / NM < 2:
                self.check(21, 'ground speed m/s', _q(rec, '160', 'GS', 'NM/s') * NM, float(speed),
                           NM * 2 ** -15 + 1e-9, row)
                self.check(21, 'track', _q(rec, '160', 'TA', '°'), float(track) % 360, 360 / 2 ** 17 + 1e-9, row)
        r = am.cat062_aircraft_record(msg, 0, 1, 1)
        if r is None:
            self.skip('CAT062 aircraft: no position')
            return
        rec = self.decode_one(am.CAT062, r)
        self.check(62, 'aircraft latitude', _q(rec, '105', 'LAT', '°'), msg.get('latitude'), 180 / 2 ** 26, row)
        self.check(62, 'aircraft longitude', _q(rec, '105', 'LON', '°'), msg.get('longitude'), 180 / 2 ** 26, row)
        if _known(alt) and alt_type == 1:
            self.check(62, 'aircraft flight level', _q(rec, '136', None, 'FL'), alt / FT / 100, 0.125 + 1e-9, row)
        if msg.get('type') in ('ADSB', 'UAT') and msg.get('address type') in (0, 2):
            self.check(62, 'aircraft address', _q(rec, '380', 'ADR'), int(msg.get('address'), 16), row=row)

    # --- drones ---

    def drone(self, msg):
        loc = msg.uas.location
        drone = decode.drone_id(msg)
        row = drone
        r = am.cat129_record(msg, 0, 1)
        if r is None:
            self.skip('CAT129: no position / altitude')
        else:
            rec = am.parse_cat129(am.raw_datablock(129, [r])[0][3:])[0]
            self.records[129] = self.records.get(129, 0) + 1
            self.check(129, 'serial (last 12)', rec.get('serial'), drone[-12:].strip(), row=row)
            self.check(129, 'ID in SP', (rec.get('SP') or b'').decode(), 'ID=' + drone, row=row)
            self.check(129, 'latitude', rec['lat'], loc.latitude, 180 / 2 ** 31 + 1e-12, row)
            self.check(129, 'longitude', rec['lon'], loc.longitude, 180 / 2 ** 31 + 1e-12, row)
            amsl = loc.altitude_baro if _known(loc.altitude_baro) else loc.altitude_geo
            if _known(amsl):
                self.check(129, 'AMSL m', rec.get('amsl'), amsl, 0.05 + 1e-6, row)
            if _known(loc.height) and loc.height_type == 1:
                self.check(129, 'AGL m', rec.get('agl'), loc.height, 0.05 + 1e-6, row)
            if _known(loc.speed_horizontal) and _known(loc.direction):
                d = math.radians(loc.direction)
                self.check(129, 'vx m/s', rec.get('vx'), loc.speed_horizontal * math.sin(d), 0.005 + 1e-6, row)
                self.check(129, 'vy m/s', rec.get('vy'), loc.speed_horizontal * math.cos(d), 0.005 + 1e-6, row)
            if _known(loc.speed_vertical):
                self.check(129, 'vv m/s', rec.get('vv'), loc.speed_vertical, 0.005 + 1e-6, row)
        r = am.cat062_drone_record(msg, 0, 1, 1)
        if r is None:
            self.skip('CAT062 drone: no position')
            return
        rec = self.decode_one(am.CAT062, r)
        self.check(62, 'drone latitude', _q(rec, '105', 'LAT', '°'), loc.latitude, 180 / 2 ** 26, row)
        self.check(62, 'drone longitude', _q(rec, '105', 'LON', '°'), loc.longitude, 180 / 2 ** 26, row)
        if _known(loc.altitude_geo):
            self.check(62, 'drone geometric altitude ft', _q(rec, '130', None, 'ft'), loc.altitude_geo / FT,
                       3.125 + 1e-9, row)
        if _known(loc.speed_horizontal) and _known(loc.direction):
            d = math.radians(loc.direction)
            self.check(62, 'drone vx m/s', _q(rec, '185', 'VX', 'm/s'), loc.speed_horizontal * math.sin(d),
                       0.125 + 1e-9, row)
            self.check(62, 'drone vy m/s', _q(rec, '185', 'VY', 'm/s'), loc.speed_horizontal * math.cos(d),
                       0.125 + 1e-9, row)

    def report(self):
        print("records encoded and decoded: %s" % ', '.join('CAT%03d %d' % kv for kv in sorted(self.records.items())))
        for why, n in sorted(self.skipped.items()):
            print("not encoded (as intended): %d x %s" % (n, why))
        print("\n%-4s %-30s %9s %7s %12s" % ('CAT', 'field', 'checked', 'wrong', 'max error'))
        for (cat, field), n in sorted(self.checked.items()):
            err = self.max_err.get((cat, field))
            print("%-4s %-30s %9d %7d %12s" % (cat, field, n, self.bad.get((cat, field), 0),
                                              '' if err is None else '%.3g' % err))
        for e in self.examples:
            print("  " + e)
        return sum(self.bad.values())


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('logs', nargs='+', help='remoteID and aircraft CSV logs')
    a = p.parse_args()
    c = Checker()
    for path in a.logs:
        kind = replay.kind_of(path)
        if kind not in ('remoteID', 'aircraft'):
            continue
        for _, m in replay.read(path):
            if kind == 'remoteID':
                c.drone(decode.decode_remote_id(m['data']))
            else:
                c.aircraft(decode.decode_aircraft(m['aircraft']))
    wrong = c.report()
    print("\n%s" % ('all values match the CSV within the item resolution' if wrong == 0 else '%d values differ' % wrong))
    sys.exit(1 if wrong else 0)


if __name__ == '__main__':
    main()

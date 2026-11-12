#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""DroneScout -> ASTERIX gateway.

Subscribes to the DroneScout MQTT messages (broker settings in config.py in the
repository root) and sends ASTERIX over UDP (settings in asterix_config.py):
CAT021 (ADS-B / UAT aircraft), CAT023 (sensor status), CAT129 (RemoteID drones),
and optionally CAT062 / CAT065 (system tracks and the gateway status).

Needs libasterix (Python >= 3.10): pip install -r integrations/asterix/requirements.txt

    python3 integrations/asterix/asterix_gateway.py [--config my_asterix_config.py] [--mqtt-config my_config.py]
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from modules import gateway                       # noqa: E402

try:
    from integrations.asterix import asterix_export   # noqa: E402
except ImportError as e:                           # libasterix missing or Python too old
    sys.exit("asterix_gateway: %s\n  pip install -r integrations/asterix/requirements.txt (Python >= 3.10)" % e)


def make_exporter(cfg=None, start=True):
    if cfg is None:
        import asterix_config as cfg

    def s(name, default=None):
        return gateway.setting(cfg, 'asterix_' + name, default)

    return asterix_export.AsterixExporter(
        asterix_export.parse_destinations(s('destinations', [])),
        sac=s('sac', 0),
        sic_table=s('sic_table', {}),
        categories=s('categories', (21, 23, 129)),
        gateway_sic=s('gateway_sic'),
        interval_s=s('interval_s', 1.0),
        heartbeat_s=s('heartbeat_s', 10),
        stale_s=s('stale_s', 10),
        cat062_aircraft=s('cat062_aircraft', False),
        cat129_country=s('cat129_country', 'ZZ'),
        cat129_destination=s('cat129_destination'),
        cat129_special_purpose=s('cat129_special_purpose', True),
        range_nm=s('range_nm'),
        service_id=s('service_id', 1),
        max_datagram=s('max_datagram', 1400),
        multicast_ttl=s('multicast_ttl', 1),
        multicast_interface=s('multicast_interface'),
        start=start,
    )


def main():
    args = gateway.arg_parser(__doc__).parse_args()
    cfg = gateway.load_settings('asterix_config', args.config, args.mqtt_config)
    try:
        exporter = make_exporter(cfg)
    except (OSError, ValueError) as e:
        sys.exit("asterix_gateway: %s (check asterix_config.py)" % e)
    print("asterix_gateway: categories %s to %s" % (sorted(exporter.categories),
                                                   ', '.join('%s:%d' % d for d in exporter.destinations)))
    if not exporter.sic_table:
        print("asterix_gateway: asterix_sic_table is empty: only CAT062/CAT065 can be sent")

    def stats():
        return ("%d datagrams, records %s, %d skipped (sensor without SIC), %d errors"
                % (exporter.sent_datagrams, {c: n for c, n in exporter.sent_records.items() if n},
                   exporter.skipped_unknown_sensor, exporter.errors))
    gateway.run('asterix_gateway', exporter, stats)


if __name__ == '__main__':
    main()

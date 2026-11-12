#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""DroneScout -> TAK gateway.

Subscribes to the DroneScout MQTT messages (broker settings in config.py in the
repository root) and sends drones, pilots, aircraft and the sensors themselves
as Cursor-on-Target to a TAK Server or to TAK clients (settings in tak_config.py).

    python3 integrations/tak/tak_gateway.py [--config my_tak_config.py] [--mqtt-config my_config.py]
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from modules import gateway                 # noqa: E402
from integrations.tak import tak_export     # noqa: E402


def make_exporter(cfg=None):
    if cfg is None:
        import tak_config as cfg

    def s(name, default=None):
        return gateway.setting(cfg, name, default)

    client = tak_export.TakClient(
        s('tak_url'),
        certfile=s('tak_client_cert'),
        keyfile=s('tak_client_key'),
        key_password=s('tak_client_key_password'),
        cafile=s('tak_ca_cert'),
        check_hostname=s('tak_check_hostname', True),
        ping_uid=s('tak_gateway_uid', 'dronescout-gateway'),
        ping_interval_s=s('tak_ping_interval_s', 60),
        presence=s('tak_presence', True),
        presence_callsign=s('tak_gateway_callsign', 'DroneScout gateway'),
        presence_position=s('tak_gateway_position'),
    )
    settings = {
        'stale_s': s('tak_stale_s'),
        'sensor_stale_s': s('tak_sensor_stale_s'),
        'type_drone': s('tak_type_drone'),
        'type_operator': s('tak_type_operator'),
        'type_sensor': s('tak_type_sensor'),
    }
    return tak_export.TAKExporter(
        client, settings,
        min_interval_s=s('tak_min_interval_s', 1.0),
        send_aircraft=s('tak_send_aircraft', True),
        send_operator=s('tak_send_operator', True),
        send_sensors=s('tak_send_sensors', True),
        sensor_positions=s('tak_sensor_positions', {}),
    )


def main():
    args = gateway.arg_parser(__doc__).parse_args()
    cfg = gateway.load_settings('tak_config', args.config, args.mqtt_config)
    try:
        exporter = make_exporter(cfg)
    except (OSError, ValueError) as e:
        sys.exit("tak_gateway: %s (check tak_config.py)" % e)
    print("tak_gateway: sending CoT to %s" % cfg.tak_url)

    def stats():
        c = exporter.client
        return ("%d sent, %d dropped while disconnected, %d skipped by the rate limit"
                % (c.sent, c.dropped, exporter.rate_limited))
    gateway.run('tak_gateway', exporter, stats)


if __name__ == '__main__':
    main()

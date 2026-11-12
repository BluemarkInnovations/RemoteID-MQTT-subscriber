#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""DroneScout -> GDL90 gateway: drones and aircraft as traffic in cockpit apps (EFBs).

Subscribes to the DroneScout MQTT messages (broker settings in config.py in the
repository root) and sends GDL90 heartbeats and traffic reports over UDP, like a
Stratux receiver (settings in gdl90_config.py).

    python3 integrations/gdl90/gdl90_gateway.py [--config my_gdl90_config.py] [--mqtt-config my_config.py]
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
from integrations.gdl90 import gdl90_export       # noqa: E402


def make_exporter(cfg=None, start=True):
    if cfg is None:
        import gdl90_config as cfg

    def s(name, default=None):
        return gateway.setting(cfg, 'gdl90_' + name, default)

    return gdl90_export.GDL90Exporter(
        gdl90_export.parse_destinations(s('destinations', [])), send_drones=s('send_drones', True),
        send_aircraft=s('send_aircraft', True), ownship=s('ownship'), ownship_sensor=s('ownship_sensor'),
        device_name=s('device_name', 'DroneSct'), device_long_name=s('device_long_name', 'DroneScout'),
        drone_nic=s('drone_nic'), start=start)


def main():
    args = gateway.arg_parser(__doc__).parse_args()
    cfg = gateway.load_settings('gdl90_config', args.config, args.mqtt_config)
    try:
        exporter = make_exporter(cfg)
    except (OSError, ValueError) as e:
        sys.exit("gdl90_gateway: %s (check gdl90_config.py)" % e)
    print("gdl90_gateway: sending to %s" % ', '.join('%s:%d' % d for d in exporter.destinations))
    if exporter.ownship or exporter.ownship_sensor:
        print("gdl90_gateway: sending an ownship position: for displays on the ground only")

    def stats():
        return "%d datagrams sent, %d errors" % (exporter.sent, exporter.errors)
    gateway.run('gdl90_gateway', exporter, stats)


if __name__ == '__main__':
    main()

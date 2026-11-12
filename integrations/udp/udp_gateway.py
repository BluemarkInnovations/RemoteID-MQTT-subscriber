#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""DroneScout -> UDP (JSON datagrams) gateway for the local network.

Subscribes to the DroneScout MQTT messages (broker settings in config.py in the
repository root) and sends every drone, aircraft and sensor status as one JSON
datagram to multicast, broadcast and/or unicast destinations (settings in udp_config.py).

    python3 integrations/udp/udp_gateway.py [--config my_udp_config.py] [--mqtt-config my_config.py]
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from modules import gateway                      # noqa: E402
from integrations.udp import udp_export          # noqa: E402


def make_exporter(cfg=None):
    if cfg is None:
        import udp_config as cfg

    def s(name, default=None):
        return gateway.setting(cfg, 'udp_' + name, default)

    return udp_export.UDPExporter(
        udp_export.parse_destinations(s('destinations', [])), source=s('source', 'dronescout-gateway'),
        min_interval_s=s('min_interval_s', 1.0), send_drones=s('send_drones', True),
        send_aircraft=s('send_aircraft', True), send_status=s('send_status', True),
        multicast_ttl=s('multicast_ttl', 1), multicast_interface=s('multicast_interface'),
        max_datagram=s('max_datagram', 1400))


def main():
    args = gateway.arg_parser(__doc__).parse_args()
    cfg = gateway.load_settings('udp_config', args.config, args.mqtt_config)
    try:
        exporter = make_exporter(cfg)
    except (OSError, ValueError) as e:
        sys.exit("udp_gateway: %s (check udp_config.py)" % e)

    def stats():
        return ("%d datagrams sent, %d errors, %d events too large, %d skipped by the rate limit"
                % (exporter.sent, exporter.errors, exporter.too_large, exporter.rate_limited))
    gateway.run('udp_gateway', exporter, stats)


if __name__ == '__main__':
    main()

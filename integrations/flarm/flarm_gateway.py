#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""DroneScout -> FLARM NMEA gateway: drones and aircraft as FLARM traffic in XCSoar, LK8000, ...

Subscribes to the DroneScout MQTT messages (broker settings in config.py in the
repository root) and serves the FLARM data port sentences ($PFLAU, $PFLAA, with the
own position as $GPRMC / $GPGGA) on a TCP port and/or over UDP (settings in flarm_config.py).

    python3 integrations/flarm/flarm_gateway.py [--config my_flarm_config.py] [--mqtt-config my_config.py]
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
from integrations.flarm import flarm_export       # noqa: E402


def make_exporter(cfg=None, start=True):
    if cfg is None:
        import flarm_config as cfg

    def s(name, default=None):
        return gateway.setting(cfg, 'flarm_' + name, default)

    return flarm_export.FLARMExporter(
        tcp_port=s('tcp_port', 2000), tcp_bind=s('tcp_bind', '0.0.0.0'),
        udp_destinations=flarm_export.parse_destinations(s('udp_destinations', []) or []),
        position=s('position'), position_sensor=s('position_sensor'), height_agl_m=s('height_agl_m', 0.0),
        send_drones=s('send_drones', True), send_aircraft=s('send_aircraft', True),
        max_range_m=s('max_range_m', 50000), stale_s=s('stale_s', 5), start=start)


def main():
    args = gateway.arg_parser(__doc__).parse_args()
    cfg = gateway.load_settings('flarm_config', args.config, args.mqtt_config)
    try:
        exporter = make_exporter(cfg)
    except (OSError, ValueError) as e:
        sys.exit("flarm_gateway: %s (check flarm_config.py)" % e)
    if exporter.server is not None:
        print("flarm_gateway: TCP server on port %d" % exporter.tcp_port)
    for host, port in exporter.udp_destinations:
        print("flarm_gateway: sending to %s:%d (UDP)" % (host, port))
    if exporter.position is None and exporter.position_sensor is None:
        print("flarm_gateway: warning: no flarm_position or flarm_position_sensor, no traffic is sent")

    def stats():
        return "%d lines sent, %d errors, %d targets out of range" % (exporter.sent_lines, exporter.errors,
                                                                       exporter.out_of_range)
    gateway.run('flarm_gateway', exporter, stats)


if __name__ == '__main__':
    main()

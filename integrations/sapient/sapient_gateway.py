#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""DroneScout -> SAPIENT (BSI Flex 335 v2.0) gateway.

Subscribes to the DroneScout MQTT messages (broker settings in config.py in the
repository root) and registers every DroneScout sensor as a SAPIENT edge node at
a fusion node / DMM / middleware: status reports as heartbeat, a detection
report per drone or aircraft update (settings in sapient_config.py).

    python3 integrations/sapient/sapient_gateway.py [--config my_sapient_config.py] [--mqtt-config my_config.py]
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from modules import gateway                         # noqa: E402
from integrations.sapient import sapient_export     # noqa: E402

_MAP_SETTINGS = ('name', 'manufacturer', 'model', 'heartbeat_s', 'coverage_m', 'class_drone',
                 'class_fixed_wing', 'class_rotorcraft', 'class_lighter_than_air', 'class_aircraft')


def make_exporter(cfg=None, start=True):
    if cfg is None:
        import sapient_config as cfg

    def s(name, default=None):
        return gateway.setting(cfg, 'sapient_' + name, default)

    return sapient_export.SAPIENTExporter(
        s('url'),
        settings={name: s(name) for name in _MAP_SETTINGS},
        stale_s=s('stale_s', 10),
        min_interval_s=s('min_interval_s', 1.0),
        detection_gap_s=s('detection_gap_s', 0.1),
        ack_timeout_s=s('ack_timeout_s', 10),
        send_aircraft=s('send_aircraft', True),
        sensor_positions=s('sensor_positions', {}),
        node_namespace=s('node_namespace', 'dronescout'),
        certfile=s('client_cert'), keyfile=s('client_key'), cafile=s('ca_cert'),
        check_hostname=s('check_hostname', True),
        start=start,
    )


def main():
    args = gateway.arg_parser(__doc__).parse_args()
    cfg = gateway.load_settings('sapient_config', args.config, args.mqtt_config)
    try:
        exporter = make_exporter(cfg)
    except (OSError, ValueError) as e:
        sys.exit("sapient_gateway: %s (check sapient_config.py)" % e)
    print("sapient_gateway: sending to %s" % cfg.sapient_url)

    def stats():
        return ("%d nodes, %d messages sent, %d detections dropped, %d skipped by the rate limit, "
                "%d tasks rejected" % (len(exporter.nodes), exporter.sent, exporter.dropped,
                                       exporter.rate_limited, exporter.tasks_rejected))
    gateway.run('sapient_gateway', exporter, stats)


if __name__ == '__main__':
    main()

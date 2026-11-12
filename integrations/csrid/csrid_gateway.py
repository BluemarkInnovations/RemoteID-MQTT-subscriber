#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""DroneScout -> DRIP CS-RID gateway (experimental): the gateway acts as a CS-RID Finder.

Subscribes to the DroneScout MQTT messages (broker settings in config.py in the
repository root), registers at an SDSP and forwards the received Broadcast RemoteID
to it over CoAP (settings in csrid_config.py).

    python3 integrations/csrid/csrid_gateway.py [--config my_csrid_config.py] [--mqtt-config my_config.py]
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
from integrations.csrid import csrid_export       # noqa: E402


def make_exporter(cfg=None, start=True):
    if cfg is None:
        import csrid_config as cfg

    def s(name, default=None):
        return gateway.setting(cfg, 'csrid_' + name, default)

    return csrid_export.CSRIDFinder(
        s('sdsp_url', 'coap://127.0.0.1:5683/csrid'), s('finder_id'), key=s('key_hex'),
        finder_mac=s('finder_mac'), position=s('position'), position_sensor=s('position_sensor'),
        min_interval_s=s('min_interval_s', 1.0), registration_retry_s=s('registration_retry_s', 30),
        start=start)


def main():
    args = gateway.arg_parser(__doc__).parse_args()
    cfg = gateway.load_settings('csrid_config', args.config, args.mqtt_config)
    try:
        exporter = make_exporter(cfg)
    except (OSError, ValueError) as e:
        sys.exit("csrid_gateway: %s (check csrid_config.py)" % e)
    print("csrid_gateway: Finder %s, SDSP coap://%s:%d/%s (experimental, draft-moskowitz-drip-crowd-sourced-rid-16)"
          % (exporter.rid, exporter.host, exporter.port, exporter.path))
    if exporter.key is None:
        print("csrid_gateway: warning: no csrid_key_hex, the messages are not authenticated")
    gateway.run('csrid_gateway', exporter, exporter.stats)


if __name__ == '__main__':
    main()

#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""DroneScout -> REST (HTTP POST, JSON) gateway.

Subscribes to the DroneScout MQTT messages (broker settings in config.py in the
repository root) and POSTs drones, aircraft and sensor status as JSON to an HTTP(S)
endpoint with bearer token, basic or API-key authentication (settings in rest_config.py).

    python3 integrations/rest/rest_gateway.py [--config my_rest_config.py] [--mqtt-config my_config.py]
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
from integrations.rest import rest_export         # noqa: E402


def make_exporter(cfg=None, start=True):
    if cfg is None:
        import rest_config as cfg

    def s(name, default=None):
        return gateway.setting(cfg, 'rest_' + name, default)

    headers = dict(s('extra_headers', {}) or {})
    headers.update(rest_export.auth_headers(s('auth'), token=s('token'), username=s('username'),
                                            password=s('password'), header_name=s('header_name'),
                                            header_value=s('header_value')))
    return rest_export.RESTExporter(
        s('url'), headers=headers, source=s('source', 'dronescout-gateway'),
        interval_s=s('interval_s', 1.0), min_interval_s=s('min_interval_s', 1.0),
        send_drones=s('send_drones', True), send_aircraft=s('send_aircraft', True),
        send_status=s('send_status', True), timeout_s=s('timeout_s', 10),
        max_queue=s('max_queue', 10000), max_batch=s('max_batch', 500),
        cafile=s('ca_cert'), verify_tls=s('verify_tls', True), start=start)


def main():
    args = gateway.arg_parser(__doc__).parse_args()
    cfg = gateway.load_settings('rest_config', args.config, args.mqtt_config)
    try:
        exporter = make_exporter(cfg)
    except (OSError, ValueError) as e:
        sys.exit("rest_gateway: %s (check rest_config.py)" % e)
    if exporter.url.startswith('http://') and gateway.setting(cfg, 'rest_auth'):
        print("rest_gateway: warning: credentials over plain http://, use https:// outside a test setup")
    print("rest_gateway: posting to %s" % exporter.url)

    def stats():
        return ("%d events posted in %d requests, %d failed requests, %d events dropped, "
                "%d skipped by the rate limit" % (exporter.posted_events, exporter.requests,
                                                  exporter.failed_requests, exporter.dropped,
                                                  exporter.rate_limited))
    gateway.run('rest_gateway', exporter, stats)


if __name__ == '__main__':
    main()

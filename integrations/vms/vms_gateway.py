#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""DroneScout -> VMS gateway: drone alarms as generic events in Milestone XProtect and
Nx Witness (Nx Meta, DW Spectrum, Wisenet WAVE).

Subscribes to the DroneScout MQTT messages (broker settings in config.py in the
repository root) and sends "drone detected", "drone lost" and sensor status changes
(settings in vms_config.py).

    python3 integrations/vms/vms_gateway.py [--config my_vms_config.py] [--mqtt-config my_config.py]
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from modules import gateway                                   # noqa: E402
from integrations.vms import vms_alarms, vms_export            # noqa: E402


def make_backends(cfg):
    def s(name, default=None):
        return gateway.setting(cfg, 'vms_' + name, default)

    backends = []
    if s('milestone_host'):
        backends.append(vms_export.MilestoneGenericEvents(
            s('milestone_host'), s('milestone_port', 1234), s('milestone_protocol', 'tcp'),
            s('milestone_template', vms_export.DEFAULT_MILESTONE_TEMPLATE)))
    if s('nx_url'):
        backends.append(vms_export.NxWitnessGenericEvents(
            s('nx_url'), s('nx_username'), s('nx_password'), source=s('nx_source', 'DroneScout'),
            camera_ids=s('nx_camera_ids', []) or [], caption=s('nx_caption'),
            description=s('nx_description', vms_export.DEFAULT_DESCRIPTION), cafile=s('nx_ca_cert'),
            verify_tls=s('nx_verify_tls', True), token=s('nx_token')))
    return backends


def make_exporter(cfg=None, start=True):
    if cfg is None:
        import vms_config as cfg
    alarms = vms_alarms.Alarms(lost_s=gateway.setting(cfg, 'vms_lost_s', 30),
                               update_s=gateway.setting(cfg, 'vms_update_s', 0),
                               aircraft=gateway.setting(cfg, 'vms_aircraft', False))
    return vms_export.VMSExporter(make_backends(cfg), alarms, start=start)


def main():
    args = gateway.arg_parser(__doc__).parse_args()
    cfg = gateway.load_settings('vms_config', args.config, args.mqtt_config)
    try:
        exporter = make_exporter(cfg)
    except (OSError, ValueError) as e:
        sys.exit("vms_gateway: %s (check vms_config.py)" % e)
    for b in exporter.backends:
        print("vms_gateway: sending to %s" % (b.url if b.name == 'nx' else '%s:%d (%s)' % (b.address + (b.protocol,))))

    def stats():
        return "sent %s, failed %s, %d dropped" % (exporter.sent, exporter.failed, exporter.dropped)
    gateway.run('vms_gateway', exporter, stats)


if __name__ == '__main__':
    main()

# (c) Bluemark Innovations BV
# MIT license
"""Shared start-up of the gateways in integrations/<standard>/.

A gateway is the MQTT subscriber without console output and CSV logs, with one
exporter registered (TAK, SAPIENT, ASTERIX, ...). Each gateway script:

    settings = gateway.load_settings('tak_config', args.config, args.mqtt_config)
    exporter = make_exporter(settings)
    gateway.run('tak_gateway', exporter, stats=lambda: '...')

The MQTT broker settings come from config.py in the repository root (or the
file given with --mqtt-config); the gateway settings from <standard>_config.py
next to the script (or the file given with --config).
"""

import argparse
import importlib.util
import os
import secrets
import signal
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None:
        raise ImportError("can't load %s" % path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[name] = module
    return module


def arg_parser(description):
    p = argparse.ArgumentParser(description=description,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('-c', '--config', help='gateway settings file (default: the *_config.py next to the script)')
    p.add_argument('--mqtt-config', help='MQTT broker settings file (default: config.py in the repository root)')
    return p


def load_settings(module_name, config_path=None, mqtt_config_path=None):
    """Load the gateway settings module (and, if given, another MQTT config.py).

    Must run before mqtt_sub is imported: mqtt_sub reads 'config' at import time."""
    if mqtt_config_path:
        _load_module('config', mqtt_config_path)
    if config_path:
        return _load_module(module_name, config_path)
    return importlib.import_module(module_name)


def setting(module, name, default=None):
    return getattr(module, name, default)


def run(name, exporter, stats=None):
    """Register the exporter and forward MQTT messages to it until SIGINT / SIGTERM."""
    import config
    import mqtt_sub
    from modules import exporters

    # the gateway only forwards: no console output per message, no CSV logs
    config.print_messages = False
    if hasattr(config, 'log_path'):
        del config.log_path
    # config.py's default client ID has only 101 values; with several gateways (and mqtt_sub.py) on
    # one broker two would share one and the broker would keep disconnecting both. A client ID
    # set on purpose (e.g. for broker ACLs) is kept.
    if str(getattr(config, 'client_id', '')).startswith('mqtt-subscriber-'):
        config.client_id = '%s-%s' % (name, secrets.token_hex(4))

    try:
        mqtt_sub.sensor_locations()                 # manual fallback positions (config.sensor_locations)
    except ValueError as e:
        sys.exit("%s: config.py: %s" % (name, e))
    exporters.register(exporter)

    def stop(signum, frame):
        queue_full = sum(exporters.dropped().values())
        exporters.shutdown()
        print("%s: stopped%s%s" % (
            name, ", " + stats() if stats else "",
            ", %d dropped (queue full)" % queue_full if queue_full else ""))
        sys.exit(0)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    client = mqtt_sub.connect_mqtt()
    client.loop_forever()

# Integrations

Gateways that pass DroneScout detections on to other systems. Each one is the MQTT
subscriber (`mqtt_sub.py`) without console output and CSV logs, with one output attached:

| Folder | Standard | Transport | Sends | Extra dependency | Test server (Docker) |
|---|---|---|---|---|---|
| [`tak/`](tak/README.md) | Cursor-on-Target (TAK) | TCP, TLS with client certificate, UDP multicast | drones, pilots, aircraft, sensors as map markers | none | FreeTAKServer 2.2.1 |
| [`sapient/`](sapient/README.md) | SAPIENT BSI Flex 335 v2.0 | TCP / TLS, protobuf | every sensor as a SAPIENT node: registration, status, detections | `protobuf` | Dstl Apex middleware |
| [`asterix/`](asterix/README.md) | EUROCONTROL ASTERIX CAT021, 023, 062, 065, 129 | UDP unicast / multicast | aircraft, drones, sensor status, system tracks | `libasterix` | receiver / decoder with pcap |
| [`rest/`](rest/README.md) | HTTP POST, JSON | HTTP / HTTPS, bearer, basic or API-key auth | drones, aircraft, sensor status and location | none | example receiver |
| [`udp/`](udp/README.md) | JSON datagrams | UDP multicast, broadcast or unicast on the local network | the same events as REST, one per datagram | none | none needed (example listener) |

Which one to use:

- **TAK**: the customer has ATAK / WinTAK / iTAK users or a TAK Server.
- **SAPIENT**: UK MOD / NATO C-UAS systems; the fusion node or DMM speaks BSI Flex 335 v2.0.
- **ASTERIX**: air traffic management and military air-picture systems.
- **REST**: anything else: your own back end, a cloud service, a database. The simplest one to start with.
- **UDP**: programs on the local network that can't use MQTT or HTTP: one JSON object per
  datagram, no server needed. No authentication: trusted networks only.

A receiver that can speak MQTT (including Node-RED) can also subscribe to the DroneScout
MQTT broker directly; the gateways are for systems that can't.

## How a gateway works

```
DroneScout sensors ──MQTT──> broker ──> <standard>_gateway.py ──> TAK / SAPIENT / ASTERIX / REST / UDP
                                        │
                                        ├─ mqtt_sub.py         MQTT subscription, message routing
                                        ├─ modules/decode.py   decodes every message once
                                        ├─ modules/exporters.py  own thread + queue per output: a slow
                                        │                        receiver never stalls MQTT
                                        └─ integrations/<standard>/<standard>_export.py
```

The MQTT broker settings are in `config.py` in the repository root, the settings of a
gateway in `<standard>_config.py` next to it. Both can be replaced on the command line,
so one installation can run several gateways with their own settings:

```
python3 integrations/tak/tak_gateway.py
python3 integrations/rest/rest_gateway.py --config /etc/dronescout/rest_site_a.py --mqtt-config /etc/dronescout/mqtt.py
```

Several gateways (and `mqtt_sub.py`) can run at the same time; each gets its own MQTT
client ID unless `client_id` in `config.py` was set to a fixed value. Stop a gateway with
Ctrl-C or SIGTERM: it then prints what it sent and dropped.

## Folder layout

Each standard has the same layout and is self-contained: nothing in one integration
depends on another one.

```
integrations/<standard>/
    README.md                  mapping, settings, limitations, how to test
    <standard>_gateway.py      start this (MQTT -> output)
    <standard>_config.py       settings of the gateway
    <standard>_export.py       the output: connection, batching, retries, rate limits
    <standard>_map.py / cot.py pure functions: decoded message -> message of the standard
                               (REST and UDP share modules/json_events.py)
    requirements.txt           extra dependencies (only where needed)
    tools/                     send_test_events.py, mock servers, decoders
    tests/                     pytest
    docker/                    compose.yaml of a test server / receiver
```

Shared code in `modules/`:

- `decode.py`: the decoded RemoteID / aircraft messages, and `drone_id()`: the stable ID of a
  drone (serial number, else another basic ID, else the MAC address) used by every output.
- `gateway.py`: command line, settings and start-up of a gateway.
- `json_events.py`: the plain JSON events of the REST and UDP outputs.
- `test_objects.py`: the synthetic test drone, aircraft and sensor used by `send_test_events.py`.

## Testing without sensors

Every integration has a `tools/send_test_events.py` that sends **made-up** objects through
the real gateway code, without MQTT or a sensor: a drone with its pilot flying a circle,
an aircraft flying east and a sensor. All IDs and callsigns contain `DRONESCOUT-TEST` and the
remarks say `TEST DATA`, so on a shared or customer system nobody takes them for real
detections. Use them to check a connection before real detections are sent.

Each integration except UDP has a Docker test target, published on localhost only (UDP
only needs `udp_listen.py`, and Docker doesn't forward broadcast or multicast):

```
docker compose -f integrations/tak/docker/compose.yaml up -d --build        # FreeTAKServer, 8087/8089
docker compose -f integrations/sapient/docker/compose.yaml up -d --build    # Apex, 5020 (nodes) / 5001 (fusion)
docker compose -f integrations/asterix/docker/compose.yaml up -d --build    # ASTERIX receiver, UDP 8600
docker compose -f integrations/rest/docker/compose.yaml up -d --build       # REST receiver, 8080
```

They are for development only. A customer's own system (a tak.gov TAK Server, a SAPIENT
fusion node, an ATM system) can behave differently: always repeat the test there with
`send_test_events.py` before switching to real data.

## Unit tests

From the repository root, with the dependencies of all integrations installed:

```
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt -r integrations/sapient/requirements.txt -r integrations/asterix/requirements.txt
.venv/bin/python -m pytest -q                       # everything (tests/ and integrations/)
.venv/bin/python -m pytest -q integrations/rest     # one integration
```

The ASTERIX tests are skipped when `libasterix` is missing (it needs Python 3.10 or newer).
The tests run against local mock servers; none of them needs Docker or the network.

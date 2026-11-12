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
| [`gdl90/`](gdl90/README.md) | GDL90 (Stratux-style) | UDP, port 4000 | drones (as UAV) and aircraft as traffic in cockpit apps: ForeFlight, Garmin Pilot, SkyDemon | none | receiver with the etdey/gdl90 decoder |
| [`flarm/`](flarm/README.md) | FLARM NMEA data port | TCP server (port 2000) and/or UDP | drones (as UAV) and aircraft as FLARM traffic: XCSoar, LK8000, SeeYou Navigator | none | receivers with the pynmea2 parser |
| [`vms/`](vms/README.md) | generic events | Milestone: TCP / UDP text; Nx Witness: REST v4 | drone detected / still present / lost, sensor degraded / normal | none | real Nx Witness 6.1 server + Milestone mock |
| [`csrid/`](csrid/README.md) | DRIP Crowd Sourced RemoteID, **experimental** (draft -16) | CoAP over UDP, COSE_Mac0 (HMAC) | the received B-RID forwarded to an SDSP, as a CS-RID Finder | none (reference SDSP: cbor2, aiocoap, pycose) | reference SDSP |

Which one to use:

- **TAK**: the customer has ATAK / WinTAK / iTAK users or a TAK Server.
- **SAPIENT**: UK MOD / NATO C-UAS systems; the fusion node or DMM speaks BSI Flex 335 v2.0.
- **ASTERIX**: air traffic management and military air-picture systems.
- **REST**: anything else: your own back end, a cloud service, a database. The simplest one to start with.
- **GDL90**: pilots' tablet apps (EFBs), e.g. a display at an airfield or a helicopter base.
- **FLARM**: gliding and GA software (XCSoar, LK8000), e.g. at a gliding club.
- **VMS**: security sites with Milestone XProtect or Nx Witness: a drone alarm starts
  recording, moves a camera or alerts the operator.
- **CS-RID**: experiments with DRIP Crowd Sourced RemoteID (U-space / UTM research). Based on
  an expired Internet-Draft; no production SDSPs exist yet.
- **UDP**: programs on the local network that can't use MQTT or HTTP: one JSON object per
  datagram, no server needed. No authentication: trusted networks only.

A receiver that can speak MQTT (including Node-RED) can also subscribe to the DroneScout
MQTT broker directly; the gateways are for systems that can't.

## How a gateway works

```
DroneScout sensors ──MQTT──> broker ──> <standard>_gateway.py ──> TAK / SAPIENT / ASTERIX / REST / UDP / GDL90 / FLARM / VMS / CS-RID
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
- `decode.pseudo_icao()` / `decode.short_callsign()`: the `FF....` address and 8-character
  callsign of a drone, shared by the SBS, GDL90 and FLARM outputs.
- `test_objects.py`: the synthetic test drone, aircraft and sensor used by `send_test_events.py`.
- `sensor_locations.py`: manual fallback position of sensors without GPS (`sensor_locations` in
  `config.py`), handed to every output as the sensor's location message.
- `replay.py`: turns the subscriber's CSV logs back into MQTT messages (replay, load test).

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
docker compose -f integrations/gdl90/docker/compose.yaml up -d --build      # GDL90 receiver (etdey/gdl90), UDP 4000
docker compose -f integrations/flarm/docker/compose.yaml up -d --build receiver-udp   # FLARM NMEA receiver (pynmea2), UDP 10110
docker compose -f integrations/vms/docker/compose.yaml up -d                # Nx Witness 7001 + Milestone mock 1234
docker compose -f integrations/csrid/docker/compose.yaml up -d --build      # CS-RID reference SDSP, CoAP UDP 5683, JSON 8088
```

Where a free decoder exists, the test receiver uses it instead of our own code (libasterix,
etdey/gdl90, pynmea2; the CS-RID SDSP uses cbor2, pycose and aiocoap), so the test also checks the encoders. They are for development only. A customer's own system (a tak.gov TAK Server, a SAPIENT
fusion node, an ATM system) can behave differently: always repeat the test there with
`send_test_events.py` before switching to real data.

## Real data and load

`modules/replay.py` turns the CSV logs (`log_path`) back into DroneScout messages: decoding a
replayed message gives the values of the CSV row (checked on 61,499 logged rows).

- `integrations/asterix/tools/replay_check.py`: replays logs through the ASTERIX encoder and
  compares the decoded records with the CSV (see the ASTERIX README).
- `tools/load_test.py`: replays logs at N x speed, optionally as N copies (other sensors, drones
  and aircraft), through `mqtt_sub`'s message handling into all outputs at once, against the
  Docker test servers. Reports per output: messages, queue drops, hand-off latency, CPU time.

```
python3 tools/load_test.py --speed 10 --duration 120               # the largest RemoteID + ADS-B log
python3 tools/load_test.py --speed 20 --copies 10 --duration 120   # stress: ~20 x the busiest log
```

Result (all 9 outputs in one process; in production each gateway is its own process),
on the largest logs (a RemoteID log with 17,606 rows, an ADS-B log with 15,995 rows):

| Run | Messages/s | CPU (one core) | Dropped | Hand-off p99 |
|---|---|---|---|---|
| 10x speed | 51 | 7 % | 0 | 2 ms |
| 20x speed, 10 copies | 1,304 | 35 % | 0 | 4-8 ms |

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

## Licences

The repository is MIT licensed. What the gateways use or include (checked 2026-11-12):

| Component | Used by | Licence | How |
|---|---|---|---|
| paho-mqtt | all | EPL-2.0 or BSD-3-Clause (EDL 1.0), our choice | pip dependency |
| bitstruct | all | MIT | pip dependency |
| protobuf | SAPIENT | BSD-3-Clause | pip dependency |
| SAPIENT proto files, BSI Flex 335 v2.0 (dstl/SAPIENT-Proto-Files) | SAPIENT | Apache 2.0, Crown copyright | **included**: `sapient/sapient_msg/` with the generated Python classes; keep its `LICENCE.txt` with them |
| libasterix | ASTERIX | MIT | pip dependency |
| CS-RID Finder, TAK, GDL90, FLARM, VMS, REST, UDP | | MIT (ours) | standard library only |

Test setups only (built locally from the Docker files, not distributed with the gateways):

| Component | Test setup | Licence |
|---|---|---|
| FreeTAKServer 2.2.1 | TAK | EPL-2.0 |
| Dstl Apex SAPIENT Middleware | SAPIENT | Apache 2.0 (Roke / Dstl, Crown copyright) |
| etdey/gdl90 | GDL90 | MIT |
| pynmea2 | FLARM | MIT |
| cbor2, aiocoap, pycose | CS-RID reference SDSP | MIT; MIT and BSD-3-Clause; BSD-3-Clause |
| ptr727/NxWitness image | VMS | MIT (the image); **Nx Witness itself is proprietary** (Network Optix EULA): test use only |
| python:3.11-slim | all receivers | PSF and the Debian packages' licences |

All of these are compatible with shipping the gateways under MIT. Apache 2.0 for the included
SAPIENT files means: keep the licence file and the copyright notice, and mark changed files
(we have none: the files are generated with `protoc`, unmodified).

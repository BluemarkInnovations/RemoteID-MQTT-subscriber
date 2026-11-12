# SAPIENT (BSI Flex 335 v2.0)

`sapient_gateway.py` connects every DroneScout sensor as a SAPIENT edge node to a fusion
node, DMM or SAPIENT middleware (e.g. Dstl Apex), using
[BSI Flex 335 v2.0](https://www.bsigroup.com/en-GB/insights-and-media/insights/brochures/bsi-flex-335-sapient/)
protobuf messages over TCP or TLS.

```
pip3 install -r integrations/sapient/requirements.txt        # protobuf
python3 integrations/sapient/sapient_gateway.py [--config my_sapient_config.py] [--mqtt-config my_config.py]
```

## What is sent

One SAPIENT node per DroneScout sensor, each over its own connection, created when the
first message of that sensor comes in:

| SAPIENT message | When | Contents |
|---|---|---|
| `Registration` | on every (re)connection | node type `PASSIVE_RF`, capabilities, status and detection definitions, mode `default` (permanent, track with re-identification); waits for the `RegistrationAck` (`sapient_ack_timeout_s`; after a rejection 60 s before trying again) |
| `StatusReport` | every `sapient_heartbeat_s` (10 s) | sensor `normal` → `SYSTEM_OK`, `degraded` → `SYSTEM_WARNING` with the reason and the state of each radio, no status yet → `SYSTEM_WARNING`; sensor position and a coverage circle of `sapient_coverage_m`; `SYSTEM_GOODBYE` when the gateway stops |
| `DetectionReport` | per drone / aircraft update, at most once per second per object | position (WGS-84 ellipsoid for RemoteID, and the accuracy as x/y/z error), ENU velocity, classification, `id`, RemoteID / ADS-B details as `object_info`, RSSI and frequency as `signal` |
| `DetectionReport`, state `lost` | when an object isn't seen for `sapient_stale_s` (10 s) | the last position |
| `TaskAck` `REJECTED` | for every `Task` | DroneScout can't be tasked |

Identifiers, in the formats Apex checks strictly:

- `node_id`: UUID (version-4 format) derived from `sapient_node_namespace` and the sensor ID,
  so a sensor keeps its node ID after a restart. Change the namespace only if two gateways
  would register the same sensor.
- `report_id`, `object_id`: ULIDs. A drone keeps its `object_id` while it is tracked; its
  RemoteID (serial number) is in `id`.

Notes on the mapping:

- **Frequencies are in MHz** (declared as such in the registration): the protobuf fields
  are float32, which can't hold 2483.5 MHz exactly in Hz.
- Bluetooth RemoteID has no channel in the message: centre 2441.75 MHz with start 2400 and
  stop 2483.5 MHz is sent.
- Aircraft altitude: GNSS height → WGS-84 ellipsoid, barometric altitude → the geoid datum
  (closest SAPIENT has to a pressure altitude).
- **Classification taxonomy is not yet verified** against BSI Flex 335 v2.0 section 7:
  drones are `Air Vehicle / UAV`, aircraft by emitter category `Fixed Wing`,
  `Rotary Wing`, `Lighter Than Air` or `Air Vehicle` without sub-class. Fusion nodes
  can use their own taxonomy: change the names with the `sapient_class_*` settings.
- Apex rejects detections less than 80 ms apart on one connection, so detections are
  paced by `sapient_detection_gap_s` (0.1 s) per node; the newest update of an object
  replaces a waiting one. At most `max_pending` (500) objects wait per node.

## Settings (`sapient_config.py`)

`sapient_url` (`tcp://host:port`, or `tls://host:port` with `sapient_client_cert`,
`sapient_client_key`, `sapient_ca_cert`), the registration names, heartbeat, coverage
radius, whether aircraft are sent, rate limits, the classification names and positions
of sensors without GPS.

## Testing

### Dstl Apex in Docker

[Apex](https://github.com/dstl/SAPIENT-Middleware) is the SAPIENT middleware from Dstl.
The test setup runs it with every BSI Flex 335 v2.0 validation on (`docker/apex_config.json`):
edge nodes connect on port 5020 ("Child"), a fusion node on port 5001 ("Peer").

```
docker compose -f integrations/sapient/docker/compose.yaml up -d --build
python3 integrations/sapient/tools/send_test_events.py tcp://127.0.0.1:5020 --duration 30 --verify-peer 127.0.0.1:5001
integrations/sapient/tools/apex_report.sh 10        # what Apex received in the last 10 minutes, and every validation error
docker compose -f integrations/sapient/docker/compose.yaml down
```

**Apex drops invalid messages silently**: the edge node gets no error. `--verify-peer`
connects as a fusion node and counts what Apex passes on, and `apex_report.sh` lists the
validation errors from Apex's database. Both should show every message forwarded and no
errors.

Apex is pinned to commit `0c8591a`. A customer's fusion node can validate differently
(taxonomy, mandatory fields, timing): repeat the test there with `send_test_events.py`,
which only sends made-up objects (`DRONESCOUT-TEST`).

### Mock fusion node and unit tests

`tools/mock_fusion_node.py` accepts registrations, records every message and checks the
mandatory fields from the proto options (`is_mandatory`, ULID / UUID formats) and the
detection gap. The unit tests use it:

```
python3 -m pytest -q integrations/sapient
```

## Proto files

`sapient_msg/` holds the BSI Flex 335 v2.0 `.proto` files from
[dstl/SAPIENT-Proto-Files](https://github.com/dstl/SAPIENT-Proto-Files) (commit `244a741`,
Apache 2.0, `LICENCE.txt`) and the Python classes generated from them. They work with
protobuf 4.25 and 5.x. To regenerate (from this folder):

```
pip3 install grpcio-tools==1.62.3
tools/generate_proto.sh
```

## Files

| File | |
|---|---|
| `sapient_gateway.py` | start this |
| `sapient_config.py` | settings |
| `sapient_export.py` | a node per sensor: connection, registration, heartbeat, pacing, lost tracks, tasks |
| `sapient_map.py` | SAPIENT messages from decoded messages (pure functions) |
| `sapient_msg/` | BSI Flex 335 v2.0 proto files and generated classes |
| `tools/send_test_events.py` | made-up objects to a fusion node, `--verify-peer` |
| `tools/mock_fusion_node.py` | checking fusion node for tests |
| `tools/apex_report.sh` | messages and validation errors in the Apex test setup |
| `tools/generate_proto.sh` | regenerate `sapient_msg/` |
| `docker/` | Dstl Apex test middleware |

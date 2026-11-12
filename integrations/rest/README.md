# REST (HTTP POST, JSON)

`rest_gateway.py` POSTs the DroneScout detections as JSON to an HTTP(S) endpoint of your
own: a back end, a cloud function, a database API. Bearer token, basic or API-key
authentication. Python standard library only.

```
python3 integrations/rest/rest_gateway.py [--config my_rest_config.py] [--mqtt-config my_config.py]
```

## The request

Once per second (`rest_interval_s`) everything that came in since the previous request
is sent as one POST (at most `rest_max_batch` events):

```
POST /dronescout/events HTTP/1.1
Content-Type: application/json
Authorization: Bearer change-me

{"schema_version": 1, "source": "dronescout-gateway", "sent": "2025-11-12T10:15:02.120Z",
 "events": [{"type": "drone", ...}, {"type": "aircraft", ...}, {"type": "sensor_status", ...}]}
```

Answer with any 2xx (the example receiver answers 204 No Content).

| Response | What the gateway does |
|---|---|
| 2xx | done |
| network error, timeout, 429, 5xx | keeps the events and retries with the next request, waiting 1, 2, 4 ... 60 s; up to `rest_max_queue` events are kept, the oldest are dropped first |
| other 4xx (400, 401, 403, 404 ...) | the request itself is wrong: the events are dropped and the error is logged (401/403: check the credentials) |

### Authentication (`rest_config.py`)

| `rest_auth` | Header sent | Settings |
|---|---|---|
| `'bearer'` | `Authorization: Bearer <token>` | `rest_token` |
| `'basic'` | `Authorization: Basic base64(user:password)` | `rest_username`, `rest_password` |
| `'header'` | `<name>: <value>`, e.g. `X-API-Key: ...` | `rest_header_name`, `rest_header_value` |
| `None` | nothing | |

Use `https://` for anything but a test: with `http://` the credentials travel in clear
text (the gateway warns about it). `rest_ca_cert` sets a private CA; `rest_verify_tls = False`
turns verification off (testing only). `rest_extra_headers` adds headers of your own.

## Event schema (version 1)

Every event has `type`, `sensor_id` (the DroneScout sensor that received it) and `time`
(UTC, ISO 8601, from the sensor). Unknown values are left out, never `null` or `NaN`.
Units are SI and part of the field name. `schema_version` only changes when a field changes
meaning; new fields can be added within a version, so ignore fields you don't know.
The same events are sent by the [UDP output](../udp/README.md) (`modules/json_events.py`).

### `drone` (RemoteID)

| Field | |
|---|---|
| `id` | stable ID: serial number, else another RemoteID basic ID, else the MAC address |
| `transport` | e.g. `WLAN Beacon`, `BT5 long range` |
| `rssi_dbm`, `channel`, `mac_address` | reception |
| `serial_number`, `uas_id`, `uas_id_type`, `ua_type` | RemoteID basic ID(s) |
| `status` | `Ground`, `Airborne`, `Emergency`, ... |
| `latitude`, `longitude` | WGS-84 degrees |
| `altitude_geodetic_m` | WGS-84 ellipsoid height |
| `altitude_pressure_m` | barometric altitude |
| `height_m`, `height_reference` | height above `ground` or `takeoff` |
| `speed_mps`, `track_deg`, `vertical_speed_mps` | |
| `horizontal_accuracy_m`, `vertical_accuracy_m` | 95 % accuracy bound |
| `operator` | `id`, `latitude`, `longitude`, `altitude_geodetic_m`, `location_type` (`Take Off`, `Dynamic`, `Fixed`) |
| `eu_category`, `eu_class` | EU classification, when sent |
| `self_id` | free-text description |
| `manufacturer`, `model` | from the serial number lookup, when known |

### `aircraft` (ADS-B, UAT, ADS-L, FLARM, OGN)

| Field | |
|---|---|
| `id` | `<technology>-<address>`, e.g. `ADSB-4840D6` |
| `technology`, `frequency_mhz`, `rssi_dbm` | reception |
| `address`, `address_type` | ICAO or other address |
| `callsign`, `emitter_category` | |
| `latitude`, `longitude` | left out when the aircraft didn't send a position |
| `altitude_m`, `altitude_type` | barometric or GNSS altitude |
| `speed_mps`, `track_deg`, `vertical_rate_mps`, `on_ground`, `emergency` | |

### `sensor_status` and `sensor_location`

`sensor_status`: `status` (`normal` / `degraded`), `status_details`, `model`,
`firmware_version`, `wlan_radios` and `sdr_receivers` (state per radio, newer firmware).
`sensor_location`: `latitude`, `longitude`, `altitude_msl_m`, `satellites`, `hdop`.

### Example

```json
{"schema_version": 1, "source": "dronescout-gateway", "sent": "2025-11-12T10:15:02.120Z", "events": [
  {"type": "drone", "id": "1596F35WXYZ", "sensor_id": "ds02261000000948", "time": "2025-11-12T10:15:01.871Z",
   "transport": "WLAN Beacon", "rssi_dbm": -71, "channel": 6, "serial_number": "1596F35WXYZ",
   "ua_type": "Helicopter (or Multirotor)", "status": "Airborne", "latitude": 52.21852, "longitude": 6.97486,
   "altitude_geodetic_m": 165.0, "height_m": 120.0, "height_reference": "takeoff", "speed_mps": 4.5,
   "track_deg": 210.0, "horizontal_accuracy_m": 10.0, "operator": {"latitude": 52.2181, "longitude": 6.9742}},
  {"type": "aircraft", "id": "ADSB-4840D6", "sensor_id": "ds02261000000948", "time": "2025-11-12T10:15:01.902Z",
   "technology": "ADSB", "frequency_mhz": 1090, "address": "4840D6", "address_type": "ICAO address via ADS-B",
   "callsign": "KLM1234", "latitude": 52.31, "longitude": 6.88, "altitude_m": 3200, "altitude_type": "baro altitude",
   "speed_mps": 180, "track_deg": 95, "on_ground": false}]}
```

## Rate limit

At most one event per drone / aircraft per `rest_min_interval_s` (1 s): a drone sends
RemoteID several times per second. Sensor status and location messages are always sent.

## Testing

### Example receiver

`tools/example_receiver.py` is a small endpoint that checks the authentication, parses the
JSON and prints one line per event. It has no dependencies (also not on this repository),
so it can be copied as the start of your own receiver.

```
python3 integrations/rest/tools/example_receiver.py --auth bearer --token change-me       # http://127.0.0.1:8080/dronescout/events
python3 integrations/rest/tools/send_test_events.py http://127.0.0.1:8080/dronescout/events --token change-me
```

Other authentication: `--auth basic --username dronescout --password change-me`, or
`--auth header --header-name X-API-Key --header-value change-me` (and `--basic user:pw` /
`--header X-API-Key:change-me` for `send_test_events.py`). HTTPS: `--cert server.pem --key server.key`
(e.g. from `integrations/tak/tools/make_test_certs.sh`), and `--cafile ca.pem` for the sender.

Or in Docker:

```
REST_TOKEN=change-me docker compose -f integrations/rest/docker/compose.yaml up -d --build
python3 integrations/rest/tools/send_test_events.py http://127.0.0.1:8080/dronescout/events --token change-me
docker compose -f integrations/rest/docker/compose.yaml logs -f receiver
docker compose -f integrations/rest/docker/compose.yaml down
```

`send_test_events.py` only sends made-up objects (`DRONESCOUT-TEST`), so it can also be
pointed at a real endpoint to check the connection and the credentials.

### Unit tests

```
python3 -m pytest -q integrations/rest
```

## Files

| File | |
|---|---|
| `rest_gateway.py` | start this |
| `rest_config.py` | settings |
| `rest_export.py` | batching, authentication headers, retries |
| `../../modules/json_events.py` | the JSON events (shared with the UDP output) |
| `tools/example_receiver.py` | example endpoint with authentication |
| `tools/send_test_events.py` | made-up objects to an endpoint |
| `docker/` | the example receiver in Docker |

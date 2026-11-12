# DRIP CS-RID: Crowd Sourced RemoteID (experimental)

`csrid_gateway.py` makes DroneScout a **Finder** in DRIP Crowd Sourced RemoteID: it
forwards the Broadcast RemoteID (B-RID) its sensors receive to an **SDSP** (Supplemental
Data Service Provider). An SDSP collects the reports of many Finders for UTM / U-space
services, e.g. to cover areas without Network RemoteID. `sdsp/` is a reference SDSP for
testing.

> **Experimental.** This follows `draft-moskowitz-drip-crowd-sourced-rid-16`, an expired
> individual Internet-Draft (not adopted by the DRIP working group) with open issues. There are
> no production SDSPs and no other implementations to test against (status 2026-11). The
> message format will change when the draft does. See "Interpretation of the draft" below.

```
python3 integrations/csrid/csrid_gateway.py [--config my_csrid_config.py] [--mqtt-config my_config.py]
```

The Finder uses the Python standard library only (its own small CBOR, COSE_Mac0 and CoAP
code). The reference SDSP uses the independent cbor2, pycose and aiocoap libraries, so
each side checks the other.

## What is sent

CoAP POST to `csrid_sdsp_url` (e.g. `coap://sdsp.example:5683/csrid`). Every message is a
CBOR `CSRID_Object` in a COSE_Mac0 (HMAC 256/256, key per Finder, key ID = Finder ID):

| Message (type) | When | CoAP |
|---|---|---|
| Finder Registration (2) | at start; again after a 4.01 / 4.03 from the SDSP | CON, retransmitted (2, 4, 8, 16 s), then retried every `csrid_registration_retry_s` |
| B-RID Forwarding (1) | each RemoteID message, at most one per drone per `csrid_min_interval_s` (1 s) | NON |
| Finder Location (4) | when no message with the Finder position went out for the update interval (default 300 s, set by the SDSP) | NON |

Nothing is forwarded until the SDSP has answered the registration.

The B-RID Forwarding message carries:

| Field | Value |
|---|---|
| `id` | `csrid_finder_id`: a DRIP Entity Tag (DET, RFC 9374) if the SDSP issued one, else any ID agreed with the SDSP |
| `timestamp` | receive time from the sensor (tdate) |
| `gps` | position of the **sensor that received** the drone [lat, lon, altitude MSL], from its location message |
| `radio_type` | 7 (UHF) for Bluetooth and 2.4 GHz Wi-Fi, 8 (SHF) for 5 GHz Wi-Fi |
| `broadcast_mac_address` | the drone's MAC address |
| `broadcast_message` | the drone's ASTM F3411 messages as a Message Pack: Basic IDs, Location, Auth pages, Self ID, System, Operator ID (re-encoded from the decoded data, see below) |
| `mac_address` | the Finder's own MAC address (`csrid_finder_mac`, default: this computer's) |

The SDSP answers in the CoAP response:

- **SDSP Response (3)**, to the registration (and whenever it changes them): its ID, the
  proxy status and the location update interval. Proxy status "reverse" stops the forwarding
  (only location updates), "forward" / "bi-directional" resumes it.
- **SDSP Heartbeat (5)**, now and then (the reference SDSP: once per Finder per hour), so the
  Finder knows its messages arrive. Counted in the statistics.

The SDSP can also POST these to the Finder's UDP port (if NAT lets it through).

## Settings (`csrid_config.py`)

SDSP URL, Finder ID, key (`csrid_key_hex`, 32 bytes hex: `python3 -c "import secrets;
print(secrets.token_hex(32))"`), the Finder position (fixed, or a sensor's GPS position),
the Finder MAC address and the forwarding rate.

Security: the messages are authenticated (HMAC) but **not encrypted**; CoAP over DTLS
(`coaps://`) is not supported yet. The SDSP rejects messages whose timestamp is more than 10
minutes off, but there is no replay protection beyond that. The forwarded data is what any
receiver near the drone can receive; the Finder position and MAC address are not, consider
that before sending to a third party.

## Testing

### Docker: reference SDSP

```
docker compose -f integrations/csrid/docker/compose.yaml up -d --build
python3 integrations/csrid/tools/send_test_events.py --duration 10 --verify http://127.0.0.1:8088
docker compose -f integrations/csrid/docker/compose.yaml logs -f sdsp
curl -s http://127.0.0.1:8088/uas          # also /finders, /messages, /stats
docker compose -f integrations/csrid/docker/compose.yaml down          # add -v to delete the message log
```

`send_test_events.py` registers as `DRONESCOUT-TEST-FINDER` with the test key from
`docker/finders.json` and forwards a test drone flying a circle; `--verify` checks the SDSP
decoded it. The SDSP log shows:

```
REGISTER DRONESCOUT-TEST-FINDER at 52.215500, 6.971900, 10 m from 172.30.0.1:45597 -> SDSP Response
B-RID DRONESCOUT-TEST-DRON via DRONESCOUT-TEST-FINDER (UHF): 52.2158434 6.9741606, 165.0 m, 5 messages
REJECT 4.01 Unauthorized from 172.30.0.1:54105: wrong MAC from DRONESCOUT-TEST-FINDER
```

With live MQTT data: set `csrid_key_hex` in `csrid_config.py` to the key of
`dronescout-finder-1` in `docker/finders.json` (test key) and start the gateway.

### Reference SDSP (`sdsp/`)

`sdsp/sdsp.py` (cbor2, aiocoap, pycose; `sdsp/requirements.txt`) and `sdsp/brid.py` (ASTM
F3411 decoder, standard library). It checks the MAC with the key of the Finder in the key ID,
the Finder ID, the timestamp, the GPS format; answers registrations, gives a heartbeat once
per Finder per `--heartbeat-s`, answers 4.01 to unknown or unregistered Finders (after an SDSP
restart the Finders then register again), keeps a table of Finders and UAs (B-RID decoded,
merged per drone MAC address) and writes every accepted message to a JSON lines file
(`--log`). `finders.json` holds the Finder IDs, keys and proxy status. A test tool, not a
production SDSP: everything is in memory, no multilateration, no UTM interface.

### Unit tests

```
python3 -m pytest -q integrations/csrid
```

CBOR against the RFC 8949 examples, COSE_Mac0 byte for byte against pycose, CoAP encoding,
the ASTM encoder against the SDSP's decoder, and the Finder against a fake SDSP:
registration, retransmission, forwarding, heartbeat, 4.01 → new registration, suspended
proxying. With cbor2, aiocoap and pycose installed (`pip install -r
integrations/csrid/sdsp/requirements.txt`), also the Finder against the reference SDSP.

## Interpretation of the draft

Draft -16 is incomplete. Choices made here, also feedback for the authors:

1. **Tag 37 on `mac_address`, `broadcast_mac_address` and `broadcast_message`.** CBOR tag 37 is
   a binary UUID (16 bytes); cbor2 refuses a 6-byte MAC address in it ("error decoding UUID").
   Sent as plain byte strings.
2. **`CSRID_Object`** lists all message members as required. Sent: `application`, `version`
   (1) and exactly one message member.
3. **Message type 5 (heartbeat)** is in Figure 1 and section 6.6 but missing from the
   `message_types` CDDL; the heartbeat CDDL has a typo (`sdsp_heartbeat_messagege`). Used as 5.
4. **The CS-RID MAC** ("MAY use COSE") has no algorithm or key. Used: COSE_Mac0, HMAC 256/256,
   key per Finder, key ID = Finder ID, the whole CBOR object as payload.
5. **`proxy_status_types`** (forward / reverse / bi) has no "suspend" value although section
   4.3 needs one. Taken: "reverse" = stop forwarding, only report the location.
6. **`radio_types`** are frequency bands (with HF twice) and can't tell Bluetooth from Wi-Fi;
   Bluetooth and 2.4 GHz Wi-Fi are both UHF. A Finder can't say which RemoteID transport it
   received.
7. **Timestamps**: section 6.2 says "a 64 bit timestamp", the CDDL `tdate` (tag 0 text). Used:
   tdate, millisecond resolution.
8. **`gps`**: the altitude's reference isn't specified. Used: metres above mean sea level.
9. **`mac_address`** in every message: whose is not specified. Used: the sender's own (Finder or
   SDSP).
10. **`update_interval`** has no unit. Used: seconds (the default is "5 minutes").
11. **Transport**: no CoAP path, port or confirmable / non-confirmable choice. Used: port 5683,
    path `csrid`, CON for the registration, NON for the rest, the SDSP's messages in the
    responses.
12. **`broadcast_message`**: the draft doesn't say if this is the air-interface frame or the
    ASTM messages. Used: the ASTM messages as a Message Pack (same for Bluetooth and Wi-Fi). The
    sensor sends decoded data (the frame only with `raw_data`), so the Finder re-encodes them;
    the values are the same, the bytes can differ from what the drone sent (e.g. reserved
    bits), so the signature of a DRIP authentication message can't be checked from them.
13. **Identity**: DETs / EdDSA keys (section 4) are not used for the MAC; `csrid_finder_id` can
    be a DET if the SDSP issued one.

## Files

| File | |
|---|---|
| `csrid_gateway.py` | start this |
| `csrid_config.py` | settings |
| `csrid_export.py` | the Finder: registration, forwarding, location updates, CoAP client |
| `csrid_map.py` | CS-RID messages (CBOR, draft -16 section 7) |
| `csrid_cbor.py` | minimal CBOR and COSE_Mac0 (HMAC-SHA256) |
| `coap.py` | minimal CoAP message encoding |
| `astm.py` | ASTM F3411-22a message encoder (Message Pack) |
| `tools/send_test_events.py` | Finder with a test drone, `--verify` against the reference SDSP |
| `sdsp/` | reference SDSP: `sdsp.py`, `brid.py`, `requirements.txt` |
| `docker/` | SDSP container, test Finder keys |

References: draft-moskowitz-drip-crowd-sourced-rid-16; RFC 8949 (CBOR), RFC 9052 / 9053
(COSE), RFC 7252 (CoAP), RFC 9374 (DET), ASTM F3411-22a.

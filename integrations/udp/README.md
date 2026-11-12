# UDP (JSON datagrams on the local network)

`udp_gateway.py` sends every drone, aircraft and sensor status as one JSON datagram to
multicast, broadcast and/or unicast destinations on the local network. Any program that
can read UDP can use it, without a server, a connection or a library: Node-RED (`udp in`
node), a few lines of Python, `nc -ul 8700`, Wireshark. Python standard library only.

```
python3 integrations/udp/udp_gateway.py [--config my_udp_config.py] [--mqtt-config my_config.py]
```

## The datagram

One event per datagram, so a receiver needs no framing: one `recvfrom()` is one
complete JSON object. The events are the ones the [REST output](../rest/README.md#event-schema-version-1)
sends, with `schema_version` and `source` added:

```json
{"schema_version":1,"source":"dronescout-gateway","type":"drone","id":"1596F35WXYZ","sensor_id":"ds02261000000948",
 "time":"2025-11-12T10:15:01.871Z","latitude":52.21852,"longitude":6.97486,"height_m":120.0, ...}
```

A drone with every field filled in is about 900 bytes, well within one Ethernet frame.
Events larger than `udp_max_datagram` (1400 bytes) are not sent, because IP fragments
often get lost. At most one datagram per drone / aircraft per second (`udp_min_interval_s`).

## Multicast, broadcast or unicast?

| | Multicast (recommended) | Broadcast | Unicast |
|---|---|---|---|
| Example (`udp_destinations`) | `239.255.86.1:8700` | `192.168.1.255:8700` | `192.168.1.20:8700` |
| Who gets it | the receivers that join the group | every host on the subnet, whether it wants it or not | one receiver per destination |
| Configuration | receivers join the group | none | the gateway needs each receiver's address |
| Switches | with IGMP snooping only the ports of receivers | flooded to every port | one port |
| Routers / VLANs | stays on the subnet with TTL 1; can be routed (TTL > 1, multicast routing) | never crosses a router | routed normally |
| Wi-Fi | sent at the lowest rate, no retransmissions: lossy, uses airtime | same | normal rate with retransmissions: the most reliable |

- **Multicast** is the usual way to share a live picture on a local network (TAK SA
  multicast and ASTERIX do the same). `239.255.0.0/16` is for use within an organisation;
  pick a group no other system on the network uses.
- **Broadcast** works without any configuration on the receiver side: good for a quick
  test or a small dedicated network. Use the subnet's broadcast address (`ip -4 addr` shows
  it after `brd`); `255.255.255.255` only leaves through the interface of the default route.
- **Unicast** for a known receiver, and the best choice over Wi-Fi. Several destinations
  can be listed; each one gets its own copy.

Something that can speak MQTT (including Node-RED) is better off subscribing to the
DroneScout MQTT broker directly: delivery guarantees, authentication and TLS. The UDP
output is for receivers that can't.

**Security**: UDP datagrams are not authenticated or encrypted; anyone on the network can
read them, and anyone can send fake ones to a receiver. Only use this output on a trusted
network segment; use the REST output (HTTPS + authentication) across untrusted networks.

## Testing

`tools/udp_listen.py` receives and prints the events. It has no dependencies (also not on
this repository), so it can be copied as the start of your own receiver.

```
# multicast
python3 integrations/udp/tools/udp_listen.py --group 239.255.86.1
python3 integrations/udp/tools/send_test_events.py 239.255.86.1:8700

# broadcast (your subnet's broadcast address)
python3 integrations/udp/tools/udp_listen.py
python3 integrations/udp/tools/send_test_events.py 192.168.1.255:8700
```

On a computer with several network interfaces, choose the one for multicast with
`--interface <its IP address>` (both tools) or `udp_multicast_interface`. Several receivers
on one computer can listen to the same port. Check the firewall of the receiver: UDP port
8700 must be open (e.g. `sudo ufw allow 8700/udp`).

`send_test_events.py` only sends made-up objects (`DRONESCOUT-TEST`), but on a shared network
every receiver of the group / subnet sees them: tell the people involved first.

There is no Docker test setup for this output: Docker's port publishing doesn't forward
broadcast or multicast, and the listener needs nothing but Python.

Unit tests:

```
python3 -m pytest -q integrations/udp
```

## Files

| File | |
|---|---|
| `udp_gateway.py` | start this |
| `udp_config.py` | settings |
| `udp_export.py` | one JSON datagram per event, to every destination |
| `../../modules/json_events.py` | the JSON events (shared with the REST output) |
| `tools/udp_listen.py` | example receiver |
| `tools/send_test_events.py` | made-up objects as UDP datagrams |

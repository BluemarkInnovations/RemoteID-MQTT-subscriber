#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings for tak_gateway.py (the MQTT broker settings are in config.py)

# Where to send the CoT:
#   tls://host:8089         TAK Server streaming port with client certificate (normal setup)
#   tcp://host:8087         TAK Server without TLS (lab only)
#   udp://239.2.3.1:6969    SA multicast to TAK clients on the local network, no server
tak_url = 'tls://takserver.example.com:8089'

# Client certificate made by the TAK Server admin (makeCert.sh client dronescout).
# TAK Server hands out .p12 files; convert them once to PEM:
#   openssl pkcs12 -legacy -in dronescout.p12 -clcerts -nokeys -out dronescout.pem
#   openssl pkcs12 -legacy -in dronescout.p12 -nocerts -nodes -out dronescout.key
#   openssl pkcs12 -legacy -in truststore-root.p12 -nokeys -out truststore-root.pem
# (leave out -legacy with OpenSSL 1.x)
tak_client_cert = './certs/dronescout.pem'
tak_client_key = './certs/dronescout.key'
#tak_client_key_password = 'atakatak'   # only when the key file is encrypted
tak_ca_cert = './certs/truststore-root.pem'

# Check that the server certificate is for tak_url's host name. Set to False when the
# server is addressed by an IP address that is not in its certificate.
tak_check_hostname = True

# What to send
tak_send_aircraft = True     # ADS-B, UAT, ADS-L, FLARM, OGN
tak_send_operator = True     # pilot position from RemoteID
tak_send_sensors = True      # the DroneScout sensors themselves

# At most one update per object per this many seconds
tak_min_interval_s = 1.0

# A marker disappears when it is not updated within this time
tak_stale_s = 10
tak_sensor_stale_s = 900

# CoT types (MIL-STD-2525 based); change them to what your TAK users expect
tak_type_drone = 'a-u-A-M-F-Q'      # unknown UAV
tak_type_operator = 'a-u-G'         # unknown ground
tak_type_sensor = 'a-f-G-E-S'       # friendly ground sensor

# Position of sensors without GPS: sensor ID -> (latitude, longitude, altitude MSL in m)
#tak_sensor_positions = {
#    'ds02261000000948': (52.21852, 6.97486, 30.0),
#}

# Keep-alive: ping the server when nothing was sent for this many seconds
tak_ping_interval_s = 60

# The gateway announces itself on every connection, like a TAK client does
# (FreeTAKServer needs this; TAK Server shows the gateway as a contact).
tak_gateway_uid = 'dronescout-gateway'          # also the uid of its ping messages
tak_gateway_callsign = 'DroneScout gateway'
#tak_gateway_position = (52.21852, 6.97486, 30.0)   # where the gateway marker is shown; default 0, 0
#tak_presence = False                             # don't announce (not recommended)

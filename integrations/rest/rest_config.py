#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings for rest_gateway.py (the MQTT broker settings are in config.py in the repository root)

# Endpoint the events are POSTed to (http:// or https://)
rest_url = 'http://127.0.0.1:8080/dronescout/events'

# Authentication: None, 'bearer', 'basic' or 'header'
#   bearer: Authorization: Bearer <rest_token>
#   basic:  Authorization: Basic base64(<rest_username>:<rest_password>)
#   header: <rest_header_name>: <rest_header_value>   (API key, e.g. X-API-Key)
# Use https:// for anything but a test: with http:// the credentials travel in clear text.
rest_auth = 'bearer'
rest_token = 'change-me'
#rest_username = 'dronescout'
#rest_password = 'change-me'
#rest_header_name = 'X-API-Key'
#rest_header_value = 'change-me'

# Extra HTTP headers, e.g. {'X-Site': 'north-gate'}
#rest_extra_headers = {}

# https:// only: CA of the server (None: the system CAs), False: don't verify (testing only)
#rest_ca_cert = './certs/ca.pem'
#rest_verify_tls = True

# "source" in every POST, to tell several gateways apart
rest_source = 'dronescout-gateway'

# What to send
rest_send_drones = True
rest_send_aircraft = True        # ADS-B, UAT, ADS-L, FLARM, OGN
rest_send_status = True          # sensor status and location messages

# One POST every rest_interval_s with what came in since the previous one (at most rest_max_batch events)
rest_interval_s = 1.0
rest_max_batch = 500

# At most one event per drone / aircraft per this many seconds (0: every message)
rest_min_interval_s = 1.0

# Request timeout, and how many events to keep while the endpoint is unreachable (oldest dropped first)
rest_timeout_s = 10
rest_max_queue = 10000

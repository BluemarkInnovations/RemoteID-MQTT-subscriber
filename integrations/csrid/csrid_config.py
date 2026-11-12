#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings for csrid_gateway.py (the MQTT broker settings are in config.py in the repository root)
#
# EXPERIMENTAL: DRIP Crowd Sourced RemoteID, draft-moskowitz-drip-crowd-sourced-rid-16 (an expired
# individual Internet-Draft). There are no production SDSPs yet; use with the reference SDSP in sdsp/.

# The SDSP (Supplemental Data Service Provider): coap://host[:port]/path. Plain CoAP over UDP; the
# messages are authenticated with the key below, not encrypted.
csrid_sdsp_url = 'coap://127.0.0.1:5683/csrid'

# The Finder ID the SDSP knows this gateway by: a DRIP Entity Tag (DET, RFC 9374) when the SDSP
# assigned one, else any ID agreed with the SDSP.
csrid_finder_id = 'dronescout-finder-1'

# Key shared with the SDSP, hex, at least 16 bytes (32 hex digits; use 32 bytes):
#   python3 -c "import secrets; print(secrets.token_hex(32))"
# None sends unauthenticated messages (the reference SDSP refuses them unless started with
# --allow-unauthenticated).
csrid_key_hex = None

# Finder position for the registration and location updates: a fixed position, or the GPS
# position of a sensor (default: of the first sensor that sends its location). Forwarded B-RID
# messages carry the position of the sensor that received them.
#csrid_position = (52.21852, 6.97486, 30.0)      # latitude, longitude, altitude MSL (m)
#csrid_position_sensor = 'ds02261000000948'

# MAC address the Finder reports as its own (mac_address of every message); default: this computer's
#csrid_finder_mac = '02:00:00:00:00:01'

# At most one B-RID forwarding message per drone per this many seconds (0: every message)
csrid_min_interval_s = 1.0

# Retry the registration after this many seconds when the SDSP doesn't answer
csrid_registration_retry_s = 30

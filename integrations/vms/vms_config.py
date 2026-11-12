#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings for vms_gateway.py (the MQTT broker settings are in config.py in the repository root)

# --- Milestone XProtect: generic events -------------------------------------------------------
# Management Client > Tools > Options > Generic Events: enable generic events; the "Compatible"
# data source listens on port 1234 (TCP and UDP). Allow the gateway's IP address there.
# Then Rules and Events > Generic Events: e.g. expression "DRONESCOUT" AND "drone_detected".
#vms_milestone_host = '192.168.1.50'
vms_milestone_port = 1234
vms_milestone_protocol = 'tcp'            # 'tcp' or 'udp'
# Text of the message: {event} {id} {sensor} {lat} {lon} {height} {speed} {pilot_lat} {pilot_lon}
# {operator} {model} {rssi} {transport} {duration} {details} {time}; unknown values are empty
#vms_milestone_template = 'DRONESCOUT {event} id={id} sensor={sensor} lat={lat} lon={lon} height={height}'

# --- Nx Witness / Nx Meta / DW Spectrum / Wisenet WAVE (6.x): generic events ------------------
# A local user with the "Advanced Viewer" role (or better) is enough.
#vms_nx_url = 'https://192.168.1.60:7001'
#vms_nx_username = 'dronescout'
#vms_nx_password = 'change-me'
#vms_nx_token = None                      # or a long-lived access token instead of user / password
vms_nx_source = 'DroneScout'              # "source" of the event, to match in event rules
# Cameras to link to the events (their IDs from the Nx web admin): rules can show / record them
vms_nx_camera_ids = []
# Nx servers use a self-signed certificate by default: give its certificate (or your CA) here,
# or set vms_nx_verify_tls = False (testing only)
#vms_nx_ca_cert = './certs/nx_server.pem'
vms_nx_verify_tls = True
# Captions per event and the description, same fields as the Milestone template
#vms_nx_caption = {'drone_detected': 'Drone detected: {id}'}
#vms_nx_description = '{event} {id} at {lat}, {lon}'

# --- Alarm logic -------------------------------------------------------------------------------
# A drone is "lost" when it isn't seen for this many seconds (a new "detected" after that)
vms_lost_s = 30
# Also send "drone_update" every this many seconds while a drone is present (0: off)
vms_update_s = 0
# Also alarms for aircraft (ADS-B, UAT, ADS-L): off, at an airfield this would be every aircraft
vms_aircraft = False

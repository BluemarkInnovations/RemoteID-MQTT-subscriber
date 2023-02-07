#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings file

import random

#MQTT broker settings
broker = '192.168.100.144'
port = 1883
topic = "#"

# generate client ID with pub prefix randomly
client_id = f'mqtt-subscriber-{random.randint(0, 100)}'

#optional user/password for connecting to the MQTT broker, uncomment if used.
#username = 'myuser'
#password = 'mypassword'

#file containing full SSL chain, uncomment when MQTT broker uses encrypted messages [preferred]
#client_pem = "./certs/client.pem"

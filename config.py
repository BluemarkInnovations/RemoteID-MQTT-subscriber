#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings file

import random

#MQTT broker settings
broker = 'myserver'
port = 8883
topic = "#"

# generate client ID with pub prefix randomly
client_id = f'mqtt-subscriber-{random.randint(0, 100)}'

#optional user/password for connecting to the MQTT broker, uncomment if used.
#username = 'myuser'
#password = 'mypassword'

#file containing full SSL chain, uncomment when MQTT broker uses encrypted messages [preferred]
#client_pem = "./certs/client.pem"

# save the detected Remote ID signals to a CSV file in the log_path folder
# uncomment to enable logging
# log_path = './logs'

#set to False to disable printing messages on the console
#print_messages = True
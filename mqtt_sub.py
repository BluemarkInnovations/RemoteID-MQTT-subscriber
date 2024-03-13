#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license

# use
# pip3 install paho-mqtt bitstruct
# to install missing modules (tested under ubuntu)


import random
import ssl
import json
import base64
from paho.mqtt import client as mqtt_client
from bitstruct import *
import lzma
import datetime
import pytz

import config # config.py with MQTT broker configuration

from modules import open_drone_id
from modules import log_remote_id

class open_drone_id_valid_blocks():
    BasicID0_valid = 0
    BasicID1_valid = 0
    LocationValid = 0
    SelfIDValid = 0
    SystemValid = 0
    OperatorIDValid = 0
    AuthValid = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ] # max 13 pages up to 255 bytes data

def connect_mqtt() -> mqtt_client:
    if hasattr(mqtt_client, 'CallbackAPIVersion'): #Set API V1 if a new paho mqtt is installed. See https://eclipse.dev/paho/files/paho.mqtt.python/html/migrations.html
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1,config.client_id)
    else:
        client = mqtt_client.Client(config.client_id)

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            subscribe(client)
        else:
            print("Failed to connect, return code %d\n", rc)

    #if username and password is set
    if hasattr(config, 'username'):
            print("username/password enabled")
            client.username_pw_set(config.username, config.password)

    #if ssl is enabled
    if hasattr(config, 'client_pem'):
            print("ssl enabled")

            #use this line if you have valid certificates
            #client.tls_set(config.client_pem, tls_version=ssl.PROTOCOL_TLSv1_2)

            #if you use *self-generated certificates*, use these lines instead:
            client.tls_set(config.client_pem, tls_version=ssl.PROTOCOL_TLSv1_2,cert_reqs=ssl.CERT_NONE)
            client.tls_insecure_set(True)

    client.on_connect = on_connect
    client.connect(config.broker, config.port)

    subscribe(client)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        if config.print_messages == True:
            print(f"Received message from topic `{msg.topic}`")
        try:
            #not compressed
            payload = msg.payload.decode()
            # remove newline (\n) char or \0 char as it will prevent decoding of json
            if ord(payload[-1:]) == 0 or ord(payload[-1:]) == 10:
                payload = payload[:-1]
        except (UnicodeDecodeError, AttributeError):
			#lzma compressed
            payload = lzma.decompress(msg.payload).decode()
            # remove \0 char as it will prevent decoding of json
            if ord(payload[-1:]) == 0:
                payload = payload[:-1]

        #print(payload) #uncomment tp print raw payload

        position = 0
        while position != -1:
            position = payload.find('}{')
            if position == -1:
                json_obj = json.loads(payload)
            else:
                message_payload = payload[0:position + 1]
                payload = payload[position + 1:]
                json_obj = json.loads(message_payload)

            try:
                if json_obj.get('protocol') == 1.0:
                    try:
                        data_json = json_obj.get('data')
                        UASdata = base64.b64decode(data_json.get('UASdata'))

                        if config.print_messages == True:
                            print("data message")
                            print("sensor ID......",  data_json.get('sensor ID'))
                            print("RSSI......",  data_json.get('RSSI'))
                            print("channel......",  data_json.get('channel'))
                            print("timestamp......",  data_json.get('timestamp'))
                            epoch_timestamp = datetime.datetime.fromtimestamp(data_json.get('timestamp')/1000, pytz.UTC)
                            print("time (of timestamp)......",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4])
                            print("MAC address......",  data_json.get('MAC address'))
                            print("type......",  data_json.get('type'))

                        valid_open_drone_id_blocks = open_drone_id_valid_blocks()
                        open_drone_id.decode_valid_blocks(UASdata, valid_open_drone_id_blocks)

                        if hasattr(config, 'log_path'):
                            log_remote_id.write_csv(data_json,UASdata, valid_open_drone_id_blocks,filename)

                        if config.print_messages == True:
                            open_drone_id.print_payload(UASdata, valid_open_drone_id_blocks)

                    except:
                        pass

                    try:
                        status_json = json_obj.get('status')
                        status_json.get('sensor ID') # fail if it is data json

                        if config.print_messages == True:
                            print("status message")
                            print("sensor ID......",  status_json.get('sensor ID'))
                            print("timestamp......",  status_json.get('timestamp'))
                            epoch_timestamp = datetime.datetime.fromtimestamp(status_json.get('timestamp')/1000)
                            print("time (local)......",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                            print("firmware version......",  status_json.get('firmware version'))
                            print("model......",  status_json.get('model'))
                            print("status......",  status_json.get('status'))
                            print("")
                    except:
                        pass
            except:
                pass

    client.subscribe(config.topic)
    client.on_message = on_message

def run():
    if not hasattr(config, 'print_messages'):
        config.print_messages = True

    if hasattr(config, 'log_path'):
        global filename
        filename = log_remote_id.open_csv(config.log_path)

    client = connect_mqtt()
    client.loop_forever()


if __name__ == '__main__':
    run()

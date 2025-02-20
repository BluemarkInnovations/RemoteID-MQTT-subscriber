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
import threading
import sys

import config # config.py with MQTT broker configuration

from modules import open_drone_id
from modules import log_remote_id
from modules import tcp_sbs_export
from modules import aircraft

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
        except:
            try:
                #lzma compressed
                payload = lzma.decompress(msg.payload).decode()
            except:
                payload = ''
        if len(payload) > 0:
            # remove newline (\n) char or \0 char as it will prevent decoding of json
            if ord(payload[-1:]) == 0 or ord(payload[-1:]) == 10:
                payload = payload[:-1]
            position = 0
        else:
            position = -1

        #print(payload) #uncomment tp print raw payload

        while position != -1:
            position = payload.find('}{')
            if position == -1:
                try:
                    json_obj = json.loads(payload)
                except:
                    json_obj = ''
            else:
                message_payload = payload[0:position + 1]
                payload = payload[position + 1:]
                try:
                    json_obj = json.loads(message_payload)
                except:
                    json_obj = ''
                    pass

            try:
                if json_obj.get('protocol') == 1.0:
                    try:
                        data_json = json_obj.get('data')
                        if (sys.getsizeof(data_json) > 16): #json exists if larger as 16 bytes
                            UASdata = base64.b64decode(data_json.get('UASdata'))

                            try:
                                raw = base64.b64decode(data_json.get('raw'))
                            except:
                                pass

                            valid_open_drone_id_blocks = open_drone_id_valid_blocks()
                            open_drone_id.decode_valid_blocks(UASdata, valid_open_drone_id_blocks)
                            try:
                                extra_json = data_json.get('extra')
                            except:
                                extra_json = ""

                            if hasattr(config, 'log_path'):
                                if 'filename_rid' not in globals():
                                    global filename_rid
                                    filename_rid = log_remote_id.open_csv(config.log_path)
                                log_remote_id.write_csv(data_json,UASdata, valid_open_drone_id_blocks,filename_rid,extra_json)

                            if config.print_messages == True:
                                open_drone_id.print_payload(UASdata, valid_open_drone_id_blocks, data_json, extra_json)

                            try:
                                tcp_sbs_export.export(UASdata, valid_open_drone_id_blocks)
                            except:
                                pass
                    except:
                        pass

                    try:
                        aircraft_json = json_obj.get('aircraft')
                        if (sys.getsizeof(aircraft_json) > 16): #json exists if larger as 16 bytes
                            if hasattr(config, 'log_path'):
                                if 'filename_aircraft' not in globals():
                                    global filename_aircraft
                                    filename_aircraft = aircraft.open_csv(config.log_path)
                                aircraft.write_csv(aircraft_json, filename_aircraft)

                        aircraft.print_payload(aircraft_json,config)

                        sbs_data = aircraft_json.get('SBS')
                        if ord(sbs_data[-1:]) == 0 or ord(sbs_data[-1:]) == 10:
                            sbs_data = sbs_data[:-1]
                        SBS_split = sbs_data.split(";")
                        for sbs_line in SBS_split:
                            try:
                                tcp_sbs_export.transmit(sbs_line + '\n')
                            except:
                                pass
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

                    try:
                        location_json = json_obj.get('location')
                        location_json.get('sensor ID') # fail if it is data json
                        if config.print_messages == True:
                            print("location message")
                            print("sensor ID............",  location_json.get('sensor ID'))
                            print("timestamp............",  location_json.get('timestamp'))
                            epoch_timestamp = datetime.datetime.fromtimestamp(location_json.get('timestamp')/1000)
                            print("time (local).........",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                            print("latitude.............",  location_json.get('latitude'))
                            print("longitude............",  location_json.get('longitude'))
                            print("altitude MSL [m].....",  location_json.get('altitude MSL'))
                            print("")
                    except:
                        pass

                    try:
                        network_json = json_obj.get('mobile network')
                        network_json.get('sensor ID') # fail if it is data json
                        if config.print_messages == True:
                            print("mobile network message")
                            print("sensor ID............",  network_json.get('sensor ID'))
                            print("timestamp............",  network_json.get('timestamp'))
                            epoch_timestamp = datetime.datetime.fromtimestamp(network_json.get('timestamp')/1000)
                            print("time (local).........",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                            print("PLMN.................",  network_json.get('PLMN'))
                            print("operator.............",  network_json.get('operator'))
                            print("band.................",  network_json.get('band'))
                            print("access technology....",  network_json.get('access technology'))
                            print("RSSI.................",  network_json.get('RSSI'))
                            print("BER..................",  network_json.get('BER'))
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

    if hasattr(config, 'sbs_server_ip_address'): # only enable SBS export if relevant vars have been defined
        if hasattr(config, 'sbs_server_port'):
            print("SBS export thread started")
            sbs_thread = threading.Thread(target=tcp_sbs_export.connect, args=(1,))
            sbs_thread.daemon = True
            sbs_thread.start()

    client = connect_mqtt()
    client.loop_forever()


if __name__ == '__main__':
    run()

#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license

# use
# pip3 install paho-mqtt bitstruct
# to install missing modules (tested under ubuntu)


import random
import ssl
import json
from paho.mqtt import client as mqtt_client
import lzma
from datetime import datetime, timezone
import threading

import config # config.py with MQTT broker configuration

from modules import open_drone_id
from modules import log_remote_id
from modules import tcp_sbs_export
from modules import aircraft
from modules import wlan_stats
from modules import status_log
from modules import decode
from modules import exporters

class open_drone_id_valid_blocks():
    def __init__(self):
        self.BasicID0_valid = 0
        self.BasicID1_valid = 0
        self.LocationValid = 0
        self.SelfIDValid = 0
        self.SystemValid = 0
        self.OperatorIDValid = 0
        self.AuthValid = [0] * 16 # max 13 pages up to 255 bytes data

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


def handle_remote_id(data_json):
    """RemoteID "data" message: decode once, then CSV, console and exporters."""
    try:
        msg = decode.decode_remote_id(data_json)
    except Exception as e:
        if config.print_messages == True:
            print("RemoteID message not decoded:", e)
        return

    if hasattr(config, 'log_path'):
        try:
            if 'filename_rid' not in globals():
                global filename_rid
                filename_rid = log_remote_id.open_csv(config.log_path)
            log_remote_id.write_csv(msg, filename_rid)
        except Exception:
            pass

    if config.print_messages == True:
        try:
            open_drone_id.print_payload(msg)
        except Exception:
            pass

    exporters.publish('on_remote_id', msg)


def handle_aircraft(aircraft_json):
    """ADS-B / UAT / ADS-L "aircraft" message: decode once, then CSV, console and exporters."""
    msg = decode.decode_aircraft(aircraft_json)

    if hasattr(config, 'log_path'):
        try:
            if 'filename_aircraft' not in globals():
                global filename_aircraft
                filename_aircraft = aircraft.open_csv(config.log_path)
            aircraft.write_csv(msg, filename_aircraft)
        except Exception:
            pass

    try:
        aircraft.print_payload(msg, config)
    except Exception:
        pass

    exporters.publish('on_aircraft', msg)


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
                    data_json = json_obj.get('data')
                    if isinstance(data_json, dict) and data_json.get('UASdata') is not None:
                        handle_remote_id(data_json)

                    aircraft_json = json_obj.get('aircraft')
                    if isinstance(aircraft_json, dict):
                        handle_aircraft(aircraft_json)

                    try:
                        status_json = json_obj.get('status')
                        status_json.get('sensor ID') # fail if it is data json
                        exporters.publish('on_status', status_json)

                        if hasattr(config, 'log_path'):
                            if 'filename_status' not in globals():
                                global filename_status
                                filename_status = status_log.open_csv(config.log_path)
                            status_log.write_csv(status_json, filename_status)

                        if config.print_messages == True:
                            print("status message")
                            print("sensor ID......",  status_json.get('sensor ID'))
                            print("timestamp......",  status_json.get('timestamp'))
                            epoch_timestamp = datetime.fromtimestamp(status_json.get('timestamp')/1000, tz=timezone.utc)
                            print("time (local)......",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                            print("firmware version......",  status_json.get('firmware version'))
                            print("model......",  status_json.get('model'))
                            print("status......",  status_json.get('status'))
                            if status_json.get('status details') is not None: # only when status is degraded
                                print("status details......",  status_json.get('status details'))
                            wlan_radios = status_json.get('WLAN radios') # e.g. {"wlan1":"ok","wlan2":"stalled"}
                            if wlan_radios is not None:
                                print("WLAN radios......",  ", ".join(f"{name}: {state}" for name, state in wlan_radios.items()))
                            sdr_receivers = status_json.get('SDR receivers') # e.g. {"1090":"ok","978":"missing"}
                            if sdr_receivers is not None:
                                print("SDR receivers......",  ", ".join(f"{name} MHz: {state}" for name, state in sdr_receivers.items()))
                            print("")
                    except:
                        pass

                    try:
                        location_json = json_obj.get('location')
                        location_json.get('sensor ID') # fail if it is data json
                        exporters.publish('on_location', location_json)
                        if config.print_messages == True:
                            print("location message")
                            print("sensor ID............",  location_json.get('sensor ID'))
                            print("timestamp............",  location_json.get('timestamp'))
                            epoch_timestamp = datetime.fromtimestamp(location_json.get('timestamp')/1000, tz=timezone.utc)
                            print("time (local).........",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                            print("latitude.............",  location_json.get('latitude'))
                            print("longitude............",  location_json.get('longitude'))
                            print("altitude MSL [m].....",  location_json.get('altitude MSL'))
                            print("number of satellites.",  location_json.get('satellites'))
                            print("HDOP.................",  location_json.get('HDOP'))
                            print("")
                    except:
                        pass

                    try:
                        stats_json = json_obj.get('wlan statistics')
                        stats_json.get('sensor ID') # fail if it is not a WLAN statistics json
                        if hasattr(config, 'log_path'):
                            if 'filename_wlan_stats' not in globals():
                                global filename_wlan_stats
                                filename_wlan_stats = wlan_stats.open_csv(config.log_path)
                            wlan_stats.write_csv(stats_json, filename_wlan_stats)
                        if config.print_messages == True:
                            wlan_stats.print_payload(stats_json)
                    except:
                        pass

                    try:
                        network_json = json_obj.get('mobile network')
                        network_json.get('sensor ID') # fail if it is data json
                        if config.print_messages == True:
                            print("mobile network message")
                            print("sensor ID............",  network_json.get('sensor ID'))
                            print("timestamp............",  network_json.get('timestamp'))
                            epoch_timestamp = datetime.fromtimestamp(network_json.get('timestamp')/1000, tz=timezone.utc)
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

def setup_exporters(threaded=True, start_sbs_listener=True):
    """Register the outputs enabled in config.py."""
    if hasattr(config, 'sbs_server_ip_address') and hasattr(config, 'sbs_server_port'): # only enable SBS export if relevant vars have been defined
        if start_sbs_listener:
            print("SBS export thread started")
            sbs_thread = threading.Thread(target=tcp_sbs_export.connect, args=(1,))
            sbs_thread.daemon = True
            sbs_thread.start()
        exporters.register(tcp_sbs_export.SBSExporter(), threaded=threaded)


def run():
    if not hasattr(config, 'print_messages'):
        config.print_messages = True

    setup_exporters()

    client = connect_mqtt()
    client.loop_forever()


if __name__ == '__main__':
    run()

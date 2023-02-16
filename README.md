# RemoteID-MQTT-subscriber

## What is it?
This repository contains reference code to subscribe to a MQTT broker and parse data from DroneScout receivers. These receivers can be purchased at https://dronescout.co/

## Installation
The code is Python3 code. It needs these dependencies (tested under Ubuntu):

```
sudo pip3 install paho-mqtt bitstruct pytz
```

## Configuration
In the config.py file, enter the correct settings for the broker, port. Leave the topic to "#" to recieve messages from all topics. Also add a username and password if your MQTT broker needs it. It is also strongly adviced to used secure SSL connection to the MQTT broker (port 8883). For that also set the client_pem file. In case of non-SSL connections, use the port 1883.

## Usage
Start the script to receive and show Remote ID signals of nearby drones.

```
python3 mqtt_sub.py
```

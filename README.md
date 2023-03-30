# RemoteID-MQTT-subscriber

## What is it?
This repository contains reference code to subscribe to a MQTT broker and parse data from DroneScout receivers (ds230). These receivers can be purchased at [https://dronescout.co/](https://dronescout.co/) The manual can be found here: [manual](https://download.bluemark.io/ds230.pdf) 

DroneScout receivers detect broadcast/direct drone Remote ID signals (Bluetooth, WLAN); a <em>"wireless number plate"</em> technology that is or becomes mandatory in several parts of the world e.g. USA, EU, Japan. It supports all frequency bands (2.4 and 5 GHz) and transmission protocols (WLAN Beacon, WLAN NaN, Bluetooth 4 Legacy, Bluetooth 5 Long Range.)

\#RemoteID \#FAA \#F3411 \#dronetechnology \#DIN_EN_4709-002

## Installation
The code is Python3 code. It needs these dependencies (tested under Ubuntu):

```
sudo pip3 install paho-mqtt bitstruct pytz
```

## Configuration
In the config.py file, enter the correct settings for the broker, port. Leave the topic to "#" to recieve messages from all topics. Also add a username and password if your MQTT broker needs it. 

It is also strongly adviced to use secure SSL connection to the MQTT broker (port 8883). For that also set the client_pem file. In case of non-SSL connections, use the port 1883. 

If you want to log the detected Remote ID signals to a CSV file, please uncomment the log_path variable.

## Usage
Start the script to receive and show Remote ID signals of nearby drones.

```
python3 mqtt_sub.py
```
 
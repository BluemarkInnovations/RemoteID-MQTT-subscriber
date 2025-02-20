import json
import base64
import csv
from datetime import datetime, timezone
from modules import tcp_sbs_export

def decode_flight_state(state):
    state =  int(state)
    state_str = "unknown"

    if state == 0:
        state_str = "ground"
    elif state == 1:
        state_str = "air"

    return state_str;

def decode_emergency_status(status):
    status =  int(status)
    status_str = "unknown"

    if status == 0:
        status_str = "none"
    elif status == 1:
        status_str = "emergency"

    return status_str;

def decode_altitude_type(alt_type):
    alt_type =  int(alt_type)
    alt_type_str = "unknown"

    if alt_type == 1:
        alt_type_str = "baro altitude"
    elif alt_type == 2:
        alt_type_str = "GNSS height"

    return alt_type_str;

def decode_address_type(address_type):

    address_type =  int(address_type)
    type_address = "unknown"

    if address_type == 0:
        type_address = "ICAO address via ADS-B"
    elif address_type == 1:
        type_address = "reserved (national use)"
    elif address_type == 2:
        type_address = "ICAO address via TIS-B"
    elif address_type == 3:
        type_address = "TIS-B track file address"
    elif address_type == 4:
        type_address = "vehicle address"
    elif address_type == 5:
        type_address = "fixed ADS-B Beacon Address"
    elif address_type == 6:
        type_address = "reserved (6)"
    elif address_type == 7:
        type_address = "reserved (7)"

    return type_address

def decode_emitter_category(category, technology):
    category =  int(category)
    category_str = "unknown"

    if (technology == "UAT") or (technology == "ADSB"):
        if category == 0:
            category_str = "no information"
        elif category == 1:
            category_str = "light <= 7000 kg"
        elif category == 2:
            category_str = "medium wake 7000 - 34000 kg"
        elif category == 3:
            category_str = "medium wake 34000 - 136000 kg"
        elif category == 4:
            category_str = "medium wake high vortex 34000 - 136000 kg"
        elif category == 5:
            category_str = "heavy >= 136000 kg"
        elif category == 6:
            category_str = "highly maneuverable"
        elif category == 7:
            category_str = "rotorcraft"
        elif category == 8:
            category_str = "reserved (8)"
        elif category == 9:
            category_str = "glider/sailplane"
        elif category == 10:
            category_str = "lighter than air"
        elif category == 11:
            category_str = "parachutist / sky diver"
        elif category == 12:
            category_str = "ultra light / hang glider / paraglider"
        elif category == 13:
            category_str = "reserved (13)"
        elif category == 14:
            category_str = "UAV"
        elif category == 15:
            category_str = "space / transatmospheric"
        elif category == 16:
            category_str = "reserved (16)"
        elif category == 17:
            category_str = "emergency vehicle"
        elif category == 18:
            category_str = "service vehicle"
        elif category == 19:
            category_str = "point obstacle"
        elif category == 20:
            category_str = "cluster obstacle"
        elif category == 21:
            category_str = "line obstacle"
        elif category == 22:
            category_str = "reserved (22)"
        elif category == 23:
            category_str = "reserved (23)"
        elif category == 24:
            category_str = "reserved (24)"
        elif category == 25:
            category_str = "reserved (25)"
        elif category == 26:
            category_str = "reserved (26)"
        elif category == 27:
            category_str = "reserved (27)"
        elif category == 28:
            category_str = "reserved (28)"
        elif category == 29:
            category_str = "reserved (29)"
        elif category == 30:
            category_str = "reserved (31)"
        elif category == 31:
            category_str = "reserved (32)"
        elif category == 32:
            category_str = "reserved (32)"
        elif category == 33:
            category_str = "reserved (33)"
        elif category == 34:
            category_str = "reserved (34)"
        elif category == 35:
            category_str = "reserved (35)"
        elif category == 36:
            category_str = "reserved (36)"
        elif category == 37:
            category_str = "reserved (37)"
        elif category == 38:
            category_str = "reserved (38)"
        elif category == 39:
            category_str = "reserved (39)"

    return category_str


def print_payload(aircraft_json, config):
    aircraft_data = base64.b64decode(aircraft_json.get('raw'))
    #remove trailing \0 character from raw data
    if ord(aircraft_data[-1:]) == 0 or ord(aircraft_data[-1:]) == 10:
        aircraft_data = aircraft_data[:-1]
    if config.print_messages == True:
        print("Aircraft message")
        print("sensor ID............",  aircraft_json.get('sensor ID'))
        print("timestamp............",  aircraft_json.get('timestamp'))
        epoch_timestamp = datetime.fromtimestamp(aircraft_json.get('timestamp')/1000, tz=timezone.utc)
        print("time (local).........",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        print("Frequency [MHz]......",  aircraft_json.get('frequency MHz'))
        print("type.................",  aircraft_json.get('type'))
        print("raw (hex)............",  aircraft_data.hex())
        addr = aircraft_json.get('address')
        addr_type = decode_address_type(aircraft_json.get('address type'))

        #replace first address characters to FF space if it is not an ICAO address
        if addr_type not in ["ICAO address via ADS-B","ICAO address via TIS-B"]:
            addr = "FF" + addr[2:]

        print("address..............",  addr)
        print("address type.........",  addr_type)
        print("flight...............",  aircraft_json.get('flight'))
        print("emergency status.....",  decode_emergency_status(aircraft_json.get('emergency status')))
        print("flight state.........",  decode_flight_state(aircraft_json.get('flight state')))
        print("category.............",  decode_emitter_category(aircraft_json.get('category'), aircraft_json.get('type')))
        print("altitude [m].........",  aircraft_json.get('altitude'))
        print("altitude type........",  decode_altitude_type(aircraft_json.get('altitude type')))
        print("speed [m/s]..........",  aircraft_json.get('speed'))
        print("track (degrees)......",  aircraft_json.get('track'))
        print("vertical rate [m/s]..",  aircraft_json.get('vertical rate'))
        print("message count........",  aircraft_json.get('message count'))

        if float(aircraft_json.get('latitude')) != 0.0:
            print("latitude.............",  aircraft_json.get('latitude'))
        else:
            print("latitude............. N/A")
        if float(aircraft_json.get('longitude')) != 0.0:
            print("longitude............",  aircraft_json.get('longitude'))
        else:
            print("longitude............ N/A")

        #print("SBS..................",  aircraft_json.get('SBS'))
        print("")


def open_csv(log_path):

    filename = log_path + '/' + datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + '_aircraft_log.csv'
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        #write header
        csv_writer.writerow(['sensor ID'] + ['timestamp'] + ['time (of timestamp)'] + ['frequency MHz']
         + ['type'] + ['address'] + ['address type'] + ['flight']  + ['emergency status']  + ['flight state'] + ['category'] + ['altitude'] + ['altitude type'] + ['speed'] + ['track']  + ['vertical rate'] + ['message count']
         + ['latitude'] + ['longitude'] + ['raw data']
         )

    return filename

def write_csv(data_json, filename):


    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        #write row
        epoch_timestamp = datetime.fromtimestamp(data_json.get('timestamp')/1000, tz=timezone.utc)

        try:
             aircraft_data = base64.b64decode(data_json.get('raw'))
             #remove trailing \0 character from raw data
             if ord(aircraft_data[-1:]) == 0 or ord(aircraft_data[-1:]) == 10:
                 aircraft_data = aircraft_data[:-1]
        except:
             pass

        csv_writer.writerow([data_json.get('sensor ID'), data_json.get('timestamp'),
            epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4], data_json.get('frequency MHz'), data_json.get('type'), data_json.get('address'), data_json.get('address type')
            , data_json.get('flight'), data_json.get('emergency status'), data_json.get('flight state'), data_json.get('category'),  data_json.get('altitude'), data_json.get('altitude type'), data_json.get('speed'), data_json.get('track')
            , data_json.get('vertical rate'), data_json.get('message count'), data_json.get('latitude'), data_json.get('longitude')
            , aircraft_data.hex()
            ])

import csv
import json
from datetime import datetime, timezone

# status message log; "status details", "WLAN radios" and "SDR receivers" are optional (newer firmware)


def open_csv(log_path):

    filename = log_path + '/' + datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + '_status_log.csv'
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        #write header
        csv_writer.writerow(['sensor ID', 'timestamp', 'time (of timestamp)', 'firmware version', 'model', 'status', 'status details',
                             'WLAN radios', 'SDR receivers'])

    return filename


def write_csv(status_json, filename):

    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        epoch_timestamp = datetime.fromtimestamp(status_json.get('timestamp')/1000, tz=timezone.utc)
        # receiver states as compact JSON in one cell, e.g. {"wlan1":"ok","wlan2":"stalled"}
        wlan_radios = status_json.get('WLAN radios')
        sdr_receivers = status_json.get('SDR receivers')
        csv_writer.writerow([status_json.get('sensor ID'), status_json.get('timestamp'), epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4],
                             status_json.get('firmware version'), status_json.get('model'), status_json.get('status'),
                             status_json.get('status details'),
                             json.dumps(wlan_radios, separators=(',', ':')) if wlan_radios is not None else None,
                             json.dumps(sdr_receivers, separators=(',', ':')) if sdr_receivers is not None else None])

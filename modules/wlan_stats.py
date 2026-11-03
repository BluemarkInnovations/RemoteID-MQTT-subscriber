import csv
from datetime import datetime, timezone

# WLAN busy-environment statistics, published by the sensor every interval (default 300 s) per WLAN radio:
# {"protocol":1.0,"wlan statistics":{"sensor ID":..,"interface":"wlan1","summary":{..},"channels":[{..}],"hardware":{..},"LTE":{..}}}

CHANNEL_FIELDS = ['channel', 'frequency MHz', 'dwell ms', 'packets', 'packets total', 'bad FCS packets', 'strong packets', 'networks',
                  'weakest signal dBm', 'median signal dBm', 'utilization %', 'noise dBm', 'noise min dBm', 'noise max dBm', 'noise rise dB']

SUMMARY_FIELDS = ['busiest channel', 'busiest channel utilization %', 'channel 6 utilization %', 'channel 6 weakest signal dBm',
                  'channel 6 noise rise dB', 'congestion']

HARDWARE_FIELDS = ['IGI avg', 'IGI max', 'IGI min', 'OFDM false alarms per s', 'CCK false alarms per s', 'CRC errors per s', 'PHY error rate %',
                   'OFDM false alarm ratio %', 'samples']

# own LTE modem: serving cell (often sub-1 GHz) and the strongest neighbour in a band next to 2.4 GHz
LTE_FIELDS = ['band', 'RSSI dBm', 'adjacent band', 'duplex', 'EARFCN', 'PCI', 'downlink MHz', 'bandwidth MHz', 'MHz from WLAN band',
              'RSRP dBm', 'RSRQ dB', 'downlink RSSI dBm', 'neighbour cells', 'adjacent band neighbour cells']

LTE_NEIGHBOUR_FIELDS = ['band', 'EARFCN', 'PCI', 'downlink MHz', 'MHz from WLAN band', 'RSRP dBm', 'downlink RSSI dBm']


def _value(value):
    # fields are left out when there is not enough data, e.g. utilization below the minimum dwell time
    if value is None:
        return "N/A"
    return value


def print_payload(stats_json):
    print("WLAN statistics message")
    print("sensor ID............",  stats_json.get('sensor ID'))
    print("timestamp............",  stats_json.get('timestamp'))
    epoch_timestamp = datetime.fromtimestamp(stats_json.get('timestamp')/1000, tz=timezone.utc)
    print("time (local).........",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    print("interface............",  stats_json.get('interface'))
    print("interval [s].........",  stats_json.get('interval s'))
    if stats_json.get('radio state') is not None:
        print("radio state..........",  stats_json.get('radio state'))
        print("last packet [s ago]..",  stats_json.get('last packet s ago'))

    summary = stats_json.get('summary') or {}
    print("congestion (ch 6)....",  _value(summary.get('congestion')))
    print("ch 6 utilization [%].",  _value(summary.get('channel 6 utilization %')))
    print("ch 6 noise rise [dB].",  _value(summary.get('channel 6 noise rise dB')))
    print("ch 6 weakest [dBm]...",  _value(summary.get('channel 6 weakest signal dBm')))
    print("busiest channel......",  _value(summary.get('busiest channel')), "(" + str(_value(summary.get('busiest channel utilization %'))) + " %)")

    channels = stats_json.get('channels') or []
    if len(channels) > 0:
        print("  ch   MHz  dwell ms  packets  bad FCS  strong  networks  util %  noise dBm  rise dB  weakest dBm")
        for ch in channels:
            print("  %-3s %5s %9s %8s %8s %7s %9s %7s %10s %8s %12s" % (
                ch.get('channel'), ch.get('frequency MHz'), ch.get('dwell ms'), ch.get('packets'), ch.get('bad FCS packets'),
                ch.get('strong packets'), ch.get('networks'), _value(ch.get('utilization %')), _value(ch.get('noise dBm')),
                _value(ch.get('noise rise dB')), _value(ch.get('weakest signal dBm'))))

    hardware = stats_json.get('hardware')
    if hardware is not None:
        print("IGI min / avg / max..",  _value(hardware.get('IGI min')), "/", hardware.get('IGI avg'), "/", hardware.get('IGI max'))
        print("false alarms [1/s]...",  "OFDM", hardware.get('OFDM false alarms per s'), "CCK", hardware.get('CCK false alarms per s'))
        print("CRC errors [1/s].....",  hardware.get('CRC errors per s'))
        print("PHY error rate [%]...",  _value(hardware.get('PHY error rate %')))
        print("OFDM FA ratio [%]....",  _value(hardware.get('OFDM false alarm ratio %')))

    lte = stats_json.get('LTE')
    if lte is not None:
        print("LTE band.............",  lte.get('band'), "(adjacent to 2.4 GHz)" if lte.get('adjacent band') else "")
        if lte.get('RSSI dBm') is not None:
            print("LTE RSSI [dBm].......",  lte.get('RSSI dBm'))
        if lte.get('EARFCN') is not None:
            print("LTE serving cell.....",  lte.get('duplex'), "EARFCN", lte.get('EARFCN'), "PCI", _value(lte.get('PCI')),
                  _value(lte.get('downlink MHz')), "MHz,", _value(lte.get('MHz from WLAN band')), "MHz from WLAN band,",
                  "RSRP", _value(lte.get('RSRP dBm')), "dBm, RSSI", _value(lte.get('downlink RSSI dBm')), "dBm")
        if lte.get('neighbour cells') is not None:
            print("LTE neighbours.......",  lte.get('neighbour cells'), "(" + str(lte.get('adjacent band neighbour cells')) + " in an adjacent band)")
        nb = lte.get('strongest adjacent band neighbour')
        if nb is not None:
            print("LTE adjacent nb......",  "band", nb.get('band'), "EARFCN", nb.get('EARFCN'), "PCI", _value(nb.get('PCI')),
                  nb.get('downlink MHz'), "MHz,", nb.get('MHz from WLAN band'), "MHz from WLAN band, RSRP", _value(nb.get('RSRP dBm')), "dBm")
    print("")


def open_csv(log_path):

    filename = log_path + '/' + datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + '_wlan_stats_log.csv'
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        #write header, one row per channel, summary/hardware/LTE repeated on each row of the interval
        csv_writer.writerow(['sensor ID', 'timestamp', 'time (of timestamp)', 'start timestamp', 'interface', 'interval s', 'radio state', 'last packet s ago']
         + CHANNEL_FIELDS + SUMMARY_FIELDS + HARDWARE_FIELDS + ['LTE ' + f for f in LTE_FIELDS]
         + ['LTE adjacent neighbour ' + f for f in LTE_NEIGHBOUR_FIELDS])

    return filename


def write_csv(stats_json, filename):

    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        epoch_timestamp = datetime.fromtimestamp(stats_json.get('timestamp')/1000, tz=timezone.utc)
        summary = stats_json.get('summary') or {}
        hardware = stats_json.get('hardware') or {}
        lte = stats_json.get('LTE') or {}

        common = [stats_json.get('sensor ID'), stats_json.get('timestamp'), epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4],
                  stats_json.get('start timestamp'), stats_json.get('interface'), stats_json.get('interval s'),
                  stats_json.get('radio state'), stats_json.get('last packet s ago')]
        nb = lte.get('strongest adjacent band neighbour') or {}
        tail = ([summary.get(f) for f in SUMMARY_FIELDS] + [hardware.get(f) for f in HARDWARE_FIELDS]
                + [lte.get(f) for f in LTE_FIELDS] + [nb.get(f) for f in LTE_NEIGHBOUR_FIELDS])

        channels = stats_json.get('channels') or [{}] # hardware-only message: one row without channel data
        for ch in channels:
            csv_writer.writerow(common + [ch.get(f) for f in CHANNEL_FIELDS] + tail)

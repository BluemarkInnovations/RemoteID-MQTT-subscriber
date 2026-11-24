import base64

# Optional per-transmitter trust verdicts, enabled by setting
# trust_verdicts = True in config.py. Uses the remoteid package
# (pip3 install remoteid) to check the raw air interface payload of each
# data message for kinematic/identity inconsistencies and DRIP
# authentication. When the package is not installed, all functions are
# no-ops and the subscriber runs as before.

try:
    import remoteid
except ImportError:
    remoteid = None

tracker = None

def available():
    return remoteid is not None

def get_tracker():
    global tracker
    if tracker is None:
        tracker = remoteid.Tracker()
    return tracker

def verdict(data_json):
    # Ingest the raw air interface payload of a data message and return a
    # trust verdict dict, or None if the message has no usable payload.
    if remoteid is None:
        return None

    raw_b64 = data_json.get('raw')
    if raw_b64 is None:
        return None

    try:
        raw = base64.b64decode(raw_b64)
        v = get_tracker().ingest(raw, "auto",
                                 address=data_json.get('MAC address'),
                                 rssi=data_json.get('RSSI'),
                                 unix_ms=data_json.get('timestamp'))
    except ValueError:
        return None # payload is not decodable Remote ID data

    return {"verdict": v.verdict,
            "reasons": v.reasons,
            "is_alert": v.is_alert,
            "uas_id": v.uas_id}

def print_verdict(v):
    print("trust verdict......", v.get('verdict'))
    if v.get('uas_id') is not None:
        print("UAS ID......", v.get('uas_id'))
    if len(v.get('reasons')) > 0:
        print("reasons......", ", ".join(v.get('reasons')))
    if v.get('is_alert') == True:
        print("ALERT...... this transmitter failed one or more trust checks")
    print("")

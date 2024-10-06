import json
from datetime import date, datetime
import numpy as np
from pandas import Timestamp

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (set, frozenset)):
            return list(obj)
        return super().default(obj)

def process_for_json(data):
    if isinstance(data, dict):
        new_dict = {}
        for k, v in data.items():
            # Convert keys to strings
            if isinstance(k, (Timestamp, datetime, date)):
                k = k.isoformat()
            else:
                k = str(k)
            new_dict[k] = process_for_json(v)
        return new_dict
    elif isinstance(data, list):
        return [process_for_json(v) for v in data]
    elif isinstance(data, (np.integer, np.floating, np.bool_)):
        return data.item()
    elif isinstance(data, np.ndarray):
        return data.tolist()
    elif isinstance(data, (set, frozenset)):
        return list(data)
    elif isinstance(data, (Timestamp, datetime, date)):
        return data.isoformat()
    else:
        return data
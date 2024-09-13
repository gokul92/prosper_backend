import json
from datetime import date, datetime
import numpy as np

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
        return {k: process_for_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [process_for_json(v) for v in data]
    elif isinstance(data, (np.integer, np.floating, np.bool_)):
        return data.item()
    elif isinstance(data, np.ndarray):
        return data.tolist()
    elif isinstance(data, (set, frozenset)):
        return list(data)
    elif isinstance(data, (date, datetime)):
        return data.isoformat()
    else:
        return data
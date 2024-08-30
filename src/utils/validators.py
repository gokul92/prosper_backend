from pydantic import ValidationError
from fastapi import HTTPException

def validate_payload(model, payload):
    try:
        return model(**payload)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal

from bson import Binary, Code, DBRef, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp


def detect_type(value) -> str:
    """Detect the BSON type of a Python value.
    
    Returns the MongoDB bsonType alias string for the value.
    """
    # Check None first
    if value is None:
        return "null"
    
    # Bool must be checked before int (bool is subclass of int in Python)
    if isinstance(value, bool):
        return "bool"
    
    # Int64 must be checked before int
    if isinstance(value, Int64):
        return "long"
    
    if isinstance(value, int):
        return "int"
    
    if isinstance(value, float):
        return "double"
    
    if isinstance(value, str):
        return "string"
    
    # DBRef must be checked before dict (DBRef has dict-like behavior)
    if isinstance(value, DBRef):
        return "dbPointer"
    
    if isinstance(value, dict):
        return "object"
    
    if isinstance(value, list):
        return "array"
    
    if isinstance(value, datetime):
        return "date"
    
    if isinstance(value, ObjectId):
        return "objectId"
    
    if isinstance(value, Decimal):
        return "decimal"
    
    if isinstance(value, Binary):
        return "binData"
    
    # Check both bson.Regex and compiled regex patterns
    if isinstance(value, Regex) or isinstance(value, re.Pattern):
        return "regex"
    
    if isinstance(value, Timestamp):
        return "timestamp"
    
    if isinstance(value, MinKey):
        return "minKey"
    
    if isinstance(value, MaxKey):
        return "maxKey"
    
    if isinstance(value, Code):
        return "javascript"
    
    if isinstance(value, bytes):
        return "binData"
    
    # Fallback for unknown types
    return "string"

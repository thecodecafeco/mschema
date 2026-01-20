from __future__ import annotations

from datetime import datetime
from bson import ObjectId
from decimal import Decimal


def detect_type(value) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
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
    return "string"

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from bson import Binary, Code, DBRef, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
from motor.motor_asyncio import AsyncIOMotorClient

from mongo_schematic.schema_io import get_schema_block


TYPE_MAP = {
    "string": str,
    "int": int,
    "double": (int, float),
    "bool": bool,
    "object": dict,
    "array": list,
    "date": datetime,
    "objectId": ObjectId,
    "decimal": Decimal,
    "long": (int, Int64),
    "binData": (bytes, Binary),
    "regex": (Regex, re.Pattern),
    "timestamp": Timestamp,
    "minKey": MinKey,
    "maxKey": MaxKey,
    "javascript": Code,
    "dbPointer": DBRef,
    "null": type(None),
}


def _expected_python_types(expected: Any) -> Tuple[type, ...]:
    if isinstance(expected, list):
        expected_types: List[type] = []
        for bson_t in expected:
            mapped = TYPE_MAP.get(bson_t)
            if mapped is None:
                continue
            if isinstance(mapped, tuple):
                expected_types.extend(list(mapped))
            else:
                expected_types.append(mapped)
        return tuple(expected_types)
    mapped = TYPE_MAP.get(expected)
    if mapped is None:
        return tuple()
    return mapped if isinstance(mapped, tuple) else (mapped,)


def _build_validator_field(field_def: Dict[str, Any]) -> Dict[str, Any]:
    bson_type = field_def.get("bsonType")
    if not bson_type:
        return {}

    validator: Dict[str, Any] = {"bsonType": bson_type}

    if bson_type == "array":
        items_def = field_def.get("items")
        if isinstance(items_def, dict) and items_def.get("bsonType"):
            items_validator: Dict[str, Any] = {"bsonType": items_def.get("bsonType")}
            if items_def.get("bsonType") == "object" and isinstance(items_def.get("properties"), dict):
                item_props = {
                    k: _build_validator_field(v)
                    for k, v in items_def.get("properties", {}).items()
                    if isinstance(v, dict)
                }
                if item_props:
                    items_validator["properties"] = item_props
            validator["items"] = items_validator

    if bson_type == "object" and isinstance(field_def.get("properties"), dict):
        obj_props = {
            k: _build_validator_field(v)
            for k, v in field_def.get("properties", {}).items()
            if isinstance(v, dict)
        }
        if obj_props:
            validator["properties"] = obj_props

    return validator


async def validate_collection(
    client: AsyncIOMotorClient,
    database: str,
    collection: str,
    schema_payload: Dict[str, Any],
    sample_size: int = 10000,
    max_errors: int = 100,
) -> Dict[str, Any]:
    db = client[database]
    coll = db[collection]

    total_docs = await coll.count_documents({})
    target = min(sample_size, total_docs) if total_docs > 0 else 0

    if target == 0:
        return {
            "database": database,
            "collection": collection,
            "total_documents": total_docs,
            "sampled_documents": 0,
            "valid": 0,
            "invalid": 0,
            "errors": [],
            "validated_at": datetime.utcnow().isoformat(),
        }

    pipeline = [{"$sample": {"size": target}}]
    docs = await coll.aggregate(pipeline).to_list(length=target)

    schema = get_schema_block(schema_payload)
    properties = schema.get("properties", {})
    required = set(schema.get("required", []))

    valid_count = 0
    errors: List[Dict[str, Any]] = []

    for doc in docs:
        doc_errors = _validate_document(doc, properties, required)
        if doc_errors:
            if len(errors) < max_errors:
                errors.append({"_id": str(doc.get("_id")), "issues": doc_errors})
        else:
            valid_count += 1

    return {
        "database": database,
        "collection": collection,
        "total_documents": total_docs,
        "sampled_documents": len(docs),
        "valid": valid_count,
        "invalid": len(docs) - valid_count,
        "errors": errors,
        "validated_at": datetime.utcnow().isoformat(),
    }


def build_mongo_validator(schema_payload: Dict[str, Any]) -> Dict[str, Any]:
    schema = get_schema_block(schema_payload)
    properties = schema.get("properties", {})
    required = schema.get("required", [])

    validator_properties: Dict[str, Any] = {}
    for field, field_def in properties.items():
        if isinstance(field_def, dict) and field_def.get("bsonType"):
            validator_properties[field] = _build_validator_field(field_def)

    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": required,
            "properties": validator_properties,
        }
    }


async def apply_validation(
    client: AsyncIOMotorClient,
    database: str,
    collection: str,
    schema_payload: Dict[str, Any],
    level: str = "moderate",
    action: str = "error",
) -> Dict[str, Any]:
    db = client[database]
    validator = build_mongo_validator(schema_payload)
    result = await db.command(
        {
            "collMod": collection,
            "validator": validator,
            "validationLevel": level,
            "validationAction": action,
        }
    )
    return {"status": "applied", "result": result}


def _validate_document(
    doc: Dict[str, Any],
    properties: Dict[str, Any],
    required: set[str],
) -> List[str]:
    issues: List[str] = []

    for field in required:
        if field not in doc or doc.get(field) is None:
            issues.append(f"Missing required field: {field}")

    for field, field_def in properties.items():
        if field not in doc or doc.get(field) is None:
            continue
        issues.extend(_validate_value(field, doc.get(field), field_def))

    return issues


def _validate_value(path: str, value: Any, field_def: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    expected = field_def.get("bsonType") if isinstance(field_def, dict) else None
    if not expected:
        return issues

    expected_types = _expected_python_types(expected)
    if expected_types and not isinstance(value, expected_types):
        issues.append(f"Type mismatch for {path}: expected {expected}")
        return issues

    # Validate object properties recursively
    if expected == "object" and isinstance(value, dict):
        props = field_def.get("properties")
        if isinstance(props, dict):
            for key, sub_def in props.items():
                if key not in value or value.get(key) is None:
                    continue
                if isinstance(sub_def, dict):
                    issues.extend(_validate_value(f"{path}.{key}", value.get(key), sub_def))

    # Strict array item validation when items are defined
    if expected == "array" and isinstance(value, list):
        items_def = field_def.get("items")
        if isinstance(items_def, dict) and items_def.get("bsonType"):
            item_expected = items_def.get("bsonType")
            item_types = _expected_python_types(item_expected)
            for idx, item in enumerate(value):
                if item_types and not isinstance(item, item_types):
                    issues.append(f"Type mismatch for {path}[{idx}]: expected {item_expected}")
                    continue
                if item_expected == "object" and isinstance(item, dict):
                    item_props = items_def.get("properties")
                    if isinstance(item_props, dict):
                        for key, sub_def in item_props.items():
                            if key not in item or item.get(key) is None:
                                continue
                            if isinstance(sub_def, dict):
                                issues.extend(
                                    _validate_value(f"{path}[{idx}].{key}", item.get(key), sub_def)
                                )

    return issues

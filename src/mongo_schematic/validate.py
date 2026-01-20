from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from bson import ObjectId
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
}


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
            validator_properties[field] = {"bsonType": field_def["bsonType"]}

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
        expected = field_def.get("bsonType") if isinstance(field_def, dict) else None
        if expected:
            expected_type = TYPE_MAP.get(expected)
            if expected_type and not isinstance(doc.get(field), expected_type):
                issues.append(f"Type mismatch for {field}: expected {expected}")

    return issues

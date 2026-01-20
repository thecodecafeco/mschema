from __future__ import annotations

from typing import Any, Dict, List

from motor.motor_asyncio import AsyncIOMotorClient

from mongo_schematic.schema_io import get_schema_block


async def list_indexes(
    client: AsyncIOMotorClient, database: str, collection: str
) -> List[Dict[str, Any]]:
    coll = client[database][collection]
    index_info = await coll.index_information()
    indexes = []
    for name, info in index_info.items():
        keys = info.get("key", [])
        indexes.append({"name": name, "keys": keys, "unique": info.get("unique", False)})
    return indexes


async def index_usage(
    client: AsyncIOMotorClient, database: str, collection: str
) -> List[Dict[str, Any]]:
    coll = client[database][collection]
    usage = await coll.aggregate([{"$indexStats": {}}]).to_list(length=1000)
    results = []
    for item in usage:
        results.append(
            {
                "name": item.get("name"),
                "key": item.get("key"),
                "accesses": item.get("accesses", {}).get("ops", 0),
            }
        )
    return results


def recommend_indexes(schema_payload: Dict[str, Any], indexes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    schema = get_schema_block(schema_payload)
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}

    existing_fields = set()
    for idx in indexes:
        for key, _direction in idx.get("keys", []):
            existing_fields.add(key)

    recommendations = []
    for field, field_def in properties.items():
        if not isinstance(field_def, dict):
            continue
        presence = field_def.get("presence", 0)
        if presence >= 0.8 and field not in existing_fields:
            recommendations.append(
                {
                    "field": field,
                    "reason": "High presence; consider indexing",
                    "suggested_index": {"fields": {field: 1}},
                }
            )

    return recommendations

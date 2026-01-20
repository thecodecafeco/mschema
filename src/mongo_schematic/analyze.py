from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List

from motor.motor_asyncio import AsyncIOMotorClient

from mongo_schematic.utils import detect_type


def _init_stats():
    return {
        "count": 0,
        "types": defaultdict(int),
        "null_count": 0,
        "sample_values": [],
    }


async def analyze_collection(
    client: AsyncIOMotorClient,
    database: str,
    collection: str,
    sample_size: int = 10000,
) -> Dict[str, Any]:
    db = client[database]
    coll = db[collection]

    total_docs = await coll.count_documents({})
    target = min(sample_size, total_docs) if total_docs > 0 else 0

    if target == 0:
        return {
            "database": database,
            "collection": collection,
            "total_documents": 0,
            "sampled_documents": 0,
            "sample_size": sample_size,
            "schema": {},
            "anomalies": [],
            "recommendations": [],
            "confidence": 0.0,
            "analyzed_at": datetime.utcnow().isoformat(),
        }

    if target == total_docs:
        docs = await coll.find().to_list(length=target)
    else:
        pipeline = [{"$sample": {"size": target}}]
        docs = await coll.aggregate(pipeline).to_list(length=target)

    field_stats: Dict[str, Dict[str, Any]] = defaultdict(_init_stats)

    for doc in docs:
        _process_document(doc, field_stats, prefix="")

    schema = _generate_schema(field_stats, total_docs)
    anomalies = _detect_anomalies(field_stats, total_docs)
    recommendations = _basic_recommendations(schema, anomalies)
    confidence = _confidence_score(field_stats, total_docs)

    return {
        "database": database,
        "collection": collection,
        "total_documents": total_docs,
        "sampled_documents": len(docs),
        "sample_size": sample_size,
        "schema": schema,
        "anomalies": anomalies,
        "recommendations": recommendations,
        "confidence": confidence,
        "analyzed_at": datetime.utcnow().isoformat(),
    }


def _process_document(
    doc: Dict[str, Any],
    stats: Dict[str, Dict[str, Any]],
    prefix: str,
    max_depth: int = 8,
    depth: int = 0,
):
    if depth >= max_depth:
        return

    for key, value in doc.items():
        if key == "_id" and not prefix:
            continue

        path = f"{prefix}.{key}" if prefix else key
        stats[path]["count"] += 1

        if value is None:
            stats[path]["null_count"] += 1
            continue

        value_type = detect_type(value)
        stats[path]["types"][value_type] += 1

        if len(stats[path]["sample_values"]) < 5:
            if value not in stats[path]["sample_values"]:
                sample_val = str(value)
                if len(sample_val) > 120:
                    sample_val = sample_val[:120] + "..."
                stats[path]["sample_values"].append(sample_val)

        if isinstance(value, dict):
            _process_document(value, stats, prefix=path, max_depth=max_depth, depth=depth + 1)
        elif isinstance(value, list) and value:
            for item in value[:5]:
                if isinstance(item, dict):
                    _process_document(
                        item,
                        stats,
                        prefix=f"{path}[]",
                        max_depth=max_depth,
                        depth=depth + 1,
                    )


def _generate_schema(field_stats: Dict[str, Dict[str, Any]], total_docs: int) -> Dict[str, Any]:
    schema = {
        "type": "object",
        "properties": {},
        "required": [],
    }

    for field, stats in field_stats.items():
        if "." in field or "[]" in field:
            continue

        if not stats["types"]:
            continue

        primary_type = max(stats["types"], key=stats["types"].get)
        presence = stats["count"] / total_docs if total_docs else 0
        null_rate = stats["null_count"] / stats["count"] if stats["count"] else 0

        schema["properties"][field] = {
            "bsonType": primary_type,
            "presence": round(presence, 4),
            "nullable": null_rate > 0,
        }

        if presence > 0.9 and null_rate < 0.1:
            schema["required"].append(field)

    return schema


def _detect_anomalies(field_stats: Dict[str, Dict[str, Any]], total_docs: int) -> List[Dict[str, Any]]:
    anomalies: List[Dict[str, Any]] = []

    for field, stats in field_stats.items():
        if len(stats["types"]) > 1:
            anomalies.append(
                {
                    "type": "MULTIPLE_TYPES",
                    "field": field,
                    "details": dict(stats["types"]),
                }
            )

        presence = stats["count"] / total_docs if total_docs else 0
        if 0 < presence < 0.05:
            anomalies.append(
                {
                    "type": "LOW_PRESENCE",
                    "field": field,
                    "details": {"presence": round(presence, 4)},
                }
            )

        if stats["count"]:
            null_rate = stats["null_count"] / stats["count"]
            if null_rate > 0.3 and presence > 0.5:
                anomalies.append(
                    {
                        "type": "HIGH_NULL_RATE",
                        "field": field,
                        "details": {"null_rate": round(null_rate, 4)},
                    }
                )

    return anomalies


def _basic_recommendations(schema: Dict[str, Any], anomalies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    recommendations: List[Dict[str, Any]] = []

    if "_schemaVersion" not in schema.get("properties", {}):
        recommendations.append(
            {
                "type": "BEST_PRACTICE",
                "title": "Add _schemaVersion field",
                "details": {
                    "field": "_schemaVersion",
                    "bsonType": "int",
                    "default": 1,
                },
            }
        )

    for anomaly in anomalies:
        if anomaly["type"] == "MULTIPLE_TYPES":
            recommendations.append(
                {
                    "type": "DATA_QUALITY",
                    "title": f"Standardize type for {anomaly['field']}",
                    "details": anomaly["details"],
                }
            )

    return recommendations


def _confidence_score(field_stats: Dict[str, Dict[str, Any]], total_docs: int) -> float:
    if not field_stats or total_docs == 0:
        return 0.0

    scores = []
    for stats in field_stats.values():
        if stats["types"]:
            max_type_count = max(stats["types"].values())
            scores.append(max_type_count / stats["count"])
        presence = stats["count"] / total_docs
        scores.append(1.0 if presence < 0.05 or presence > 0.95 else min(presence, 1 - presence) * 2)

    return round(sum(scores) / len(scores), 3) if scores else 0.0

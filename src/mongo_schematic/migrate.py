from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from mongo_schematic.diff import diff_schemas
from mongo_schematic.schema_io import get_schema_block

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from bson import ObjectId


def _generate_up_code(diff: Dict[str, Any], to_schema: Dict[str, Any], collection: str) -> str:
    """Generate the up() method code based on schema diff."""
    lines: List[str] = []
    schema = get_schema_block(to_schema)
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
    
    added = diff.get("added_fields", [])
    removed = diff.get("removed_fields", [])
    changed = diff.get("changed_fields", [])
    
    if not added and not removed and not changed:
        lines.append("        # No changes detected")
        lines.append("        pass")
        return "\n".join(lines)
    
    lines.append(f"        coll = self.db['{collection}']")
    lines.append("")
    
    # Handle added fields
    for field in added:
        field_def = properties.get(field, {})
        default = field_def.get("default") if isinstance(field_def, dict) else None
        
        if default is not None:
            default_repr = json.dumps(default)
            lines.append(f"        # Add field '{field}' with default value")
            lines.append(f"        await coll.update_many(")
            lines.append(f"            {{'{field}': {{'$exists': False}}}},")
            lines.append(f"            {{'$set': {{'{field}': {default_repr}}}}}")
            lines.append(f"        )")
            lines.append("")
        else:
            bson_type = field_def.get("bsonType", "null") if isinstance(field_def, dict) else "null"
            default_for_type = _default_for_bson_type(bson_type)
            lines.append(f"        # Add field '{field}' - TODO: verify default value")
            lines.append(f"        await coll.update_many(")
            lines.append(f"            {{'{field}': {{'$exists': False}}}},")
            lines.append(f"            {{'$set': {{'{field}': {default_for_type}}}}}")
            lines.append(f"        )")
            lines.append("")
    
    # Handle type conversions
    for change in changed:
        field = change.get("field", "unknown")
        to_def = change.get("to", {})
        to_type = to_def.get("bsonType") if isinstance(to_def, dict) else None
        
        if to_type:
            mongo_type = _bson_to_mongo_convert_type(to_type)
            lines.append(f"        # Convert '{field}' to {to_type}")
            lines.append(f"        await coll.update_many(")
            lines.append(f"            {{'{field}': {{'$exists': True}}}},")
            lines.append(f"            [{{")
            lines.append(f"                '$set': {{")
            lines.append(f"                    '{field}': {{")
            lines.append(f"                        '$convert': {{")
            lines.append(f"                            'input': '${field}',")
            lines.append(f"                            'to': '{mongo_type}',")
            lines.append(f"                            'onError': '${field}',")
            lines.append(f"                            'onNull': None")
            lines.append(f"                        }}")
            lines.append(f"                    }}")
            lines.append(f"                }}")
            lines.append(f"            }}]")
            lines.append(f"        )")
            lines.append("")
    
    # Handle removed fields (commented out for safety)
    for field in removed:
        lines.append(f"        # Uncomment to remove field '{field}' (DESTRUCTIVE)")
        lines.append(f"        # await coll.update_many(")
        lines.append(f"        #     {{'{field}': {{'$exists': True}}}},")
        lines.append(f"        #     {{'$unset': {{'{field}': ''}}}}")
        lines.append(f"        # )")
        lines.append("")
    
    return "\n".join(lines)


def _generate_down_code(diff: Dict[str, Any], from_schema: Dict[str, Any], collection: str) -> str:
    """Generate the down() method code (rollback) based on schema diff."""
    lines: List[str] = []
    schema = get_schema_block(from_schema)
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
    
    added = diff.get("added_fields", [])
    removed = diff.get("removed_fields", [])
    changed = diff.get("changed_fields", [])
    
    if not added and not removed and not changed:
        lines.append("        # No changes to rollback")
        lines.append("        pass")
        return "\n".join(lines)
    
    lines.append(f"        coll = self.db['{collection}']")
    lines.append("")
    
    # Reverse: remove added fields
    for field in added:
        lines.append(f"        # Remove field '{field}' (was added in up)")
        lines.append(f"        await coll.update_many(")
        lines.append(f"            {{'{field}': {{'$exists': True}}}},")
        lines.append(f"            {{'$unset': {{'{field}': ''}}}}")
        lines.append(f"        )")
        lines.append("")
    
    # Reverse: restore removed fields (commented - needs manual data)
    for field in removed:
        lines.append(f"        # Restore field '{field}' - requires backup data")
        lines.append(f"        # TODO: Implement data restoration for '{field}'")
        lines.append(f"        pass")
        lines.append("")
    
    # Reverse: convert types back
    for change in changed:
        field = change.get("field", "unknown")
        from_def = change.get("from", {})
        from_type = from_def.get("bsonType") if isinstance(from_def, dict) else None
        
        if from_type:
            mongo_type = _bson_to_mongo_convert_type(from_type)
            lines.append(f"        # Revert '{field}' to {from_type}")
            lines.append(f"        await coll.update_many(")
            lines.append(f"            {{'{field}': {{'$exists': True}}}},")
            lines.append(f"            [{{")
            lines.append(f"                '$set': {{")
            lines.append(f"                    '{field}': {{")
            lines.append(f"                        '$convert': {{")
            lines.append(f"                            'input': '${field}',")
            lines.append(f"                            'to': '{mongo_type}',")
            lines.append(f"                            'onError': '${field}',")
            lines.append(f"                            'onNull': None")
            lines.append(f"                        }}")
            lines.append(f"                    }}")
            lines.append(f"                }}")
            lines.append(f"            }}]")
            lines.append(f"        )")
            lines.append("")
    
    return "\n".join(lines)


def _default_for_bson_type(bson_type: str) -> str:
    """Return a Python repr of a sensible default for a BSON type."""
    defaults = {
        "string": '""',
        "int": "0",
        "double": "0.0",
        "bool": "False",
        "array": "[]",
        "object": "{}",
        "date": "datetime.utcnow()",
        "null": "None",
    }
    return defaults.get(bson_type, "None")


def _bson_to_mongo_convert_type(bson_type: str) -> str:
    """Map BSON type names to MongoDB $convert type values."""
    mapping = {
        "string": "string",
        "int": "int",
        "double": "double",
        "bool": "bool",
        "objectId": "objectId",
        "date": "date",
        "long": "long",
        "decimal": "decimal",
    }
    return mapping.get(bson_type, "string")


def generate_migration_file(
    from_schema: Dict[str, Any],
    to_schema: Dict[str, Any],
    collection: str,
    out_path: Path,
) -> Path:
    """Generate an executable migration file from schema diff.
    
    Args:
        from_schema: Source schema (current state).
        to_schema: Target schema (desired state).
        collection: Name of the MongoDB collection.
        out_path: Path to write the migration file.
        
    Returns:
        Path to the generated migration file.
    """
    diff = diff_schemas(from_schema, to_schema)
    generated_at = datetime.utcnow().isoformat()
    
    up_code = _generate_up_code(diff, to_schema, collection)
    down_code = _generate_down_code(diff, from_schema, collection)
    
    meta = {
        "collection": collection,
        "generated_at": generated_at,
        "summary": diff.get("summary", {}),
        "added_fields": diff.get("added_fields", []),
        "removed_fields": diff.get("removed_fields", []),
        "changed_fields": [c.get("field") for c in diff.get("changed_fields", [])],
    }

    content = f'''"""
Migration: {collection} schema change
Generated at: {generated_at}

Summary:
  Added fields: {len(diff.get("added_fields", []))}
  Removed fields: {len(diff.get("removed_fields", []))}
  Changed fields: {len(diff.get("changed_fields", []))}

Usage:
    from motor.motor_asyncio import AsyncIOMotorClient
    
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    migration = Migration(client, "your_database")
    await migration.up()  # Apply migration
    await migration.down()  # Rollback (if needed)
"""

from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient


class Migration:
    """Schema migration for {collection} collection."""
    
    def __init__(self, client: AsyncIOMotorClient, database: str):
        self.client = client
        self.db = client[database]

    async def up(self):
        \"\"\"Apply the forward migration.\"\"\"
{up_code}

    async def down(self):
        \"\"\"Rollback the migration.\"\"\"
{down_code}


__metadata__ = {repr(meta)}
'''

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content)
    return out_path


def generate_migration_plan(from_schema: Dict[str, Any], to_schema: Dict[str, Any]) -> Dict[str, Any]:
    diff = diff_schemas(from_schema, to_schema)
    steps = []
    for field in diff.get("added_fields", []):
        steps.append({"action": "add_field", "field": field, "details": {}})
    for field in diff.get("removed_fields", []):
        steps.append({"action": "remove_field", "field": field, "details": {}})
    for change in diff.get("changed_fields", []):
        steps.append(
            {
                "action": "convert_type",
                "field": change["field"],
                "details": {"from": change.get("from"), "to": change.get("to")},
            }
        )
    return {
        "strategy": "eager",
        "batch_size": 1000,
        "steps": steps,
        "summary": diff.get("summary", {}),
    }


async def apply_migration_plan(
    client: AsyncIOMotorClient,
    database: str,
    collection: str,
    plan: Dict[str, Any],
    to_schema: Dict[str, Any],
    allow_remove: bool = False,
    dry_run: bool = False,
    rate_limit_ms: int = 0,
    resume_from: Any = None,
) -> Dict[str, Any]:
    coll = client[database][collection]
    schema = get_schema_block(to_schema)
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}

    batch_size = int(plan.get("batch_size", 1000))
    summary = {"updated": 0, "skipped": 0, "errors": 0}
    step_results: List[Dict[str, Any]] = []

    if plan.get("strategy") == "lazy":
        return {"summary": summary, "steps": [], "dry_run": dry_run, "strategy": "lazy"}

    resume_value = _parse_resume_id(resume_from)

    for step in plan.get("steps", []):
        action = step.get("action")
        field = step.get("field")
        if not field or not action:
            summary["skipped"] += 1
            continue

        if action == "remove_field" and not allow_remove:
            step_results.append({"action": action, "field": field, "skipped": True})
            summary["skipped"] += 1
            continue

        if action == "add_field":
            default = _get_default(properties, field)
            if default is None:
                step_results.append({"action": action, "field": field, "skipped": True})
                summary["skipped"] += 1
                continue
            updated, last_id = await _batched_update(
                coll,
                {field: {"$exists": False}},
                {"$set": {field: default}},
                batch_size,
                dry_run,
                rate_limit_ms,
                resume_value,
            )
            step_results.append(
                {"action": action, "field": field, "updated": updated, "last_id": last_id}
            )
            summary["updated"] += updated
            continue

        if action == "remove_field":
            updated, last_id = await _batched_update(
                coll,
                {field: {"$exists": True}},
                {"$unset": {field: ""}},
                batch_size,
                dry_run,
                rate_limit_ms,
                resume_value,
            )
            step_results.append(
                {"action": action, "field": field, "updated": updated, "last_id": last_id}
            )
            summary["updated"] += updated
            continue

        if action == "convert_type":
            to_type = _get_bson_type(properties, field)
            if not to_type:
                step_results.append({"action": action, "field": field, "skipped": True})
                summary["skipped"] += 1
                continue
            updated, last_id = await _batched_convert(
                coll,
                field,
                to_type,
                batch_size,
                dry_run,
                rate_limit_ms,
                resume_value,
            )
            step_results.append(
                {"action": action, "field": field, "updated": updated, "last_id": last_id}
            )
            summary["updated"] += updated
            continue

        if action == "rename_field":
            new_name = step.get("details", {}).get("to")
            if not new_name:
                step_results.append({"action": action, "field": field, "skipped": True})
                summary["skipped"] += 1
                continue
            updated, last_id = await _batched_update(
                coll,
                {field: {"$exists": True}},
                {"$rename": {field: new_name}},
                batch_size,
                dry_run,
                rate_limit_ms,
                resume_value,
            )
            step_results.append(
                {"action": action, "field": field, "updated": updated, "last_id": last_id}
            )
            summary["updated"] += updated
            continue

        step_results.append({"action": action, "field": field, "skipped": True})
        summary["skipped"] += 1

    return {"summary": summary, "steps": step_results, "dry_run": dry_run}


def _get_default(properties: Dict[str, Any], field: str) -> Any:
    field_def = properties.get(field)
    if isinstance(field_def, dict):
        return field_def.get("default")
    return None


def _get_bson_type(properties: Dict[str, Any], field: str) -> str | None:
    field_def = properties.get(field)
    if isinstance(field_def, dict):
        return field_def.get("bsonType")
    return None


async def _batched_update(
    coll,
    query: Dict[str, Any],
    update: Dict[str, Any],
    batch_size: int,
    dry_run: bool,
    rate_limit_ms: int,
    resume_from: Any = None,
) -> tuple[int, Any]:
    updated = 0
    last_id = None
    if resume_from is not None:
        query = {**query, "_id": {"$gt": resume_from}}
    cursor = coll.find(query, projection=["_id"]).batch_size(batch_size)
    batch_ids: List[Any] = []

    async for doc in cursor:
        last_id = doc["_id"]
        batch_ids.append(doc["_id"])
        if len(batch_ids) >= batch_size:
            if not dry_run:
                await coll.update_many({"_id": {"$in": batch_ids}}, update)
            updated += len(batch_ids)
            batch_ids = []
            if rate_limit_ms > 0:
                await _sleep_ms(rate_limit_ms)

    if batch_ids:
        if not dry_run:
            await coll.update_many({"_id": {"$in": batch_ids}}, update)
        updated += len(batch_ids)

    return updated, last_id


async def _batched_convert(
    coll,
    field: str,
    to_type: str,
    batch_size: int,
    dry_run: bool,
    rate_limit_ms: int,
    resume_from: Any = None,
) -> tuple[int, Any]:
    updated = 0
    last_id = None
    query = {field: {"$exists": True}}
    if resume_from is not None:
        query = {**query, "_id": {"$gt": resume_from}}
    cursor = coll.find(query, projection=["_id"]).batch_size(batch_size)
    ops: List[UpdateOne] = []

    async for doc in cursor:
        last_id = doc["_id"]
        if dry_run:
            updated += 1
            continue

        ops.append(
            UpdateOne(
                {"_id": doc["_id"]},
                [
                    {
                        "$set": {
                            field: {
                                "$convert": {
                                    "input": f"${field}",
                                    "to": to_type,
                                    "onError": f"${field}",
                                    "onNull": None,
                                }
                            }
                        }
                    }
                ],
            )
        )

        if len(ops) >= batch_size:
            result = await coll.bulk_write(ops, ordered=False)
            updated += result.modified_count
            ops = []
            if rate_limit_ms > 0:
                await _sleep_ms(rate_limit_ms)

    if ops and not dry_run:
        result = await coll.bulk_write(ops, ordered=False)
        updated += result.modified_count

    return updated, last_id


async def _sleep_ms(ms: int) -> None:
    import asyncio

    await asyncio.sleep(ms / 1000)


def _parse_resume_id(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return ObjectId(value)
        except Exception:
            return value
    return value

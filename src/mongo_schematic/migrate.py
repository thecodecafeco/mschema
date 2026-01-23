from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set

from mongo_schematic.diff import diff_schemas
from mongo_schematic.schema_io import get_schema_block

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from bson import ObjectId


def _generate_up_code(
    diff: Dict[str, Any],
    to_schema: Dict[str, Any],
    collection: str,
    from_schema: Dict[str, Any] | None = None,
) -> str:
    """Generate the up() method code based on schema diff."""
    lines: List[str] = []
    schema = get_schema_block(to_schema)
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
    source_schema = get_schema_block(from_schema) if from_schema else {}
    source_props = source_schema.get("properties", {}) if isinstance(source_schema, dict) else {}
    
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
    
    # Handle required field additions
    to_required = set(schema.get("required", [])) if isinstance(schema, dict) else set()
    from_required = set(source_schema.get("required", [])) if isinstance(source_schema, dict) else set()
    new_required = sorted(to_required - from_required)

    for field in new_required:
        field_def = properties.get(field, {}) if isinstance(properties.get(field), dict) else {}
        default = field_def.get("default")
        if default is not None:
            default_repr = json.dumps(default)
            lines.append(f"        # Fill missing required field '{field}' with default")
            lines.append(f"        await coll.update_many(")
            lines.append(f"            {{'$or': [{{'{field}': {{'$exists': False}}}}, {{'{field}': None}}]}},")
            lines.append(f"            {{'$set': {{'{field}': {default_repr}}}}}")
            lines.append(f"        )")
            lines.append("")
        else:
            lines.append(f"        # Required field '{field}' has no default; manual backfill required")
            lines.append("")

    # Handle nullable -> non-nullable changes
    for field, to_def in properties.items():
        if field not in source_props:
            continue
        from_def = source_props.get(field, {}) if isinstance(source_props.get(field), dict) else {}
        if from_def.get("nullable") is True and to_def.get("nullable") is False:
            default = to_def.get("default")
            if default is not None:
                default_repr = json.dumps(default)
                lines.append(f"        # Fill nulls for '{field}' with default")
                lines.append(f"        await coll.update_many(")
                lines.append(f"            {{'{field}': None}},")
                lines.append(f"            {{'$set': {{'{field}': {default_repr}}}}}")
                lines.append(f"        )")
                lines.append("")
            else:
                lines.append(f"        # '{field}' is now non-nullable; manual backfill required")
                lines.append("")

    # Handle type conversions
    for change in changed:
        field = change.get("field", "unknown")
        from_def = change.get("from", {})
        to_def = change.get("to", {})
        from_type = from_def.get("bsonType") if isinstance(from_def, dict) else None
        to_type = to_def.get("bsonType") if isinstance(to_def, dict) else None
        
        if not to_type:
            continue

        if isinstance(to_type, list):
            if _normalize_types(from_def).issubset(_normalize_types(to_def)):
                lines.append(f"        # '{field}' widened to union {to_type}; no data migration needed")
                lines.append("")
                continue
            lines.append(f"        # '{field}' changed to union {to_type}; manual migration required")
            lines.append("")
            continue

        if to_type == "array" and from_type != "array":
            lines.append(f"        # Wrap '{field}' into array")
            lines.append(f"        await coll.update_many(")
            lines.append(f"            {{'{field}': {{'$exists': True}}}},")
            lines.append(f"            [{{")
            lines.append(f"                '$set': {{")
            lines.append(f"                    '{field}': {{")
            lines.append(f"                        '$cond': [")
            lines.append(f"                            {{'$isArray': '${field}'}},")
            lines.append(f"                            '${field}',")
            lines.append(f"                            ['${field}']")
            lines.append(f"                        ]")
            lines.append(f"                    }}")
            lines.append(f"                }}")
            lines.append(f"            }}]")
            lines.append(f"        )")
            lines.append("")
            continue

        if from_type == "array" and to_type != "array":
            lines.append(f"        # Unwrap '{field}' from array")
            lines.append(f"        await coll.update_many(")
            lines.append(f"            {{'{field}': {{'$exists': True}}}},")
            lines.append(f"            [{{")
            lines.append(f"                '$set': {{")
            lines.append(f"                    '{field}': {{")
            lines.append(f"                        '$cond': [")
            lines.append(f"                            {{'$isArray': '${field}'}},")
            lines.append(f"                            {{'$arrayElemAt': ['${field}', 0]}},")
            lines.append(f"                            '${field}'")
            lines.append(f"                        ]")
            lines.append(f"                    }}")
            lines.append(f"                }}")
            lines.append(f"            }}]")
            lines.append(f"        )")
            lines.append("")
            continue

        if to_type == "array" and from_type == "array":
            from_item = _get_items_bson_type(from_def)
            to_item = _get_items_bson_type(to_def)
            if from_item and to_item and from_item != to_item and isinstance(to_item, str):
                mongo_type = _bson_to_mongo_convert_type(to_item)
                lines.append(f"        # Convert '{field}' array items to {to_item}")
                lines.append(f"        await coll.update_many(")
                lines.append(f"            {{'{field}': {{'$exists': True}}}},")
                lines.append(f"            [{{")
                lines.append(f"                '$set': {{")
                lines.append(f"                    '{field}': {{")
                lines.append(f"                        '$cond': [")
                lines.append(f"                            {{'$isArray': '${field}'}},")
                lines.append(f"                            {{'$map': {{")
                lines.append(f"                                'input': '${field}',")
                lines.append(f"                                'as': 'item',")
                lines.append(f"                                'in': {{")
                lines.append(
                    f"                                    '$convert': {{'input': '$$item', 'to': '{mongo_type}', 'onError': '$$item', 'onNull': None}}"
                )
                lines.append(f"                                }}")
                lines.append(f"                            }}},")
                lines.append(f"                            '${field}'")
                lines.append(f"                        ]")
                lines.append(f"                    }}")
                lines.append(f"                }}")
                lines.append(f"            }}]")
                lines.append(f"        )")
                lines.append("")
                continue

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
        to_def = change.get("to", {})
        to_type = to_def.get("bsonType") if isinstance(to_def, dict) else None
        
        if not from_type:
            continue

        if isinstance(from_type, list):
            lines.append(f"        # '{field}' reverted to union {from_type}; manual rollback required")
            lines.append("")
            continue

        if from_type == "array" and to_type != "array":
            lines.append(f"        # Wrap '{field}' into array (rollback)")
            lines.append(f"        await coll.update_many(")
            lines.append(f"            {{'{field}': {{'$exists': True}}}},")
            lines.append(f"            [{{")
            lines.append(f"                '$set': {{")
            lines.append(f"                    '{field}': {{")
            lines.append(f"                        '$cond': [")
            lines.append(f"                            {{'$isArray': '${field}'}},")
            lines.append(f"                            '${field}',")
            lines.append(f"                            ['${field}']")
            lines.append(f"                        ]")
            lines.append(f"                    }}")
            lines.append(f"                }}")
            lines.append(f"            }}]")
            lines.append(f"        )")
            lines.append("")
            continue

        if from_type != "array" and to_type == "array":
            lines.append(f"        # Unwrap '{field}' from array (rollback)")
            lines.append(f"        await coll.update_many(")
            lines.append(f"            {{'{field}': {{'$exists': True}}}},")
            lines.append(f"            [{{")
            lines.append(f"                '$set': {{")
            lines.append(f"                    '{field}': {{")
            lines.append(f"                        '$cond': [")
            lines.append(f"                            {{'$isArray': '${field}'}},")
            lines.append(f"                            {{'$arrayElemAt': ['${field}', 0]}},")
            lines.append(f"                            '${field}'")
            lines.append(f"                        ]")
            lines.append(f"                    }}")
            lines.append(f"                }}")
            lines.append(f"            }}]")
            lines.append(f"        )")
            lines.append("")
            continue

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


def _normalize_types(definition: Dict[str, Any]) -> Set[str]:
    if not isinstance(definition, dict):
        return set()
    bson_type = definition.get("bsonType")
    if isinstance(bson_type, list):
        return {t for t in bson_type if isinstance(t, str)}
    if isinstance(bson_type, str):
        return {bson_type}
    return set()


def _get_items_bson_type(field_def: Dict[str, Any]) -> Any:
    if not isinstance(field_def, dict):
        return None
    items = field_def.get("items")
    if isinstance(items, dict):
        return items.get("bsonType")
    return None


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
    
    up_code = _generate_up_code(diff, to_schema, collection, from_schema)
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
    source_schema = get_schema_block(from_schema)
    target_schema = get_schema_block(to_schema)
    source_props = source_schema.get("properties", {}) if isinstance(source_schema, dict) else {}
    target_props = target_schema.get("properties", {}) if isinstance(target_schema, dict) else {}
    source_required = set(source_schema.get("required", [])) if isinstance(source_schema, dict) else set()
    target_required = set(target_schema.get("required", [])) if isinstance(target_schema, dict) else set()
    steps = []
    for field in diff.get("added_fields", []):
        steps.append({"action": "add_field", "field": field, "details": {}})
    for field in diff.get("removed_fields", []):
        steps.append({"action": "remove_field", "field": field, "details": {}})
    for change in diff.get("changed_fields", []):
        field = change["field"]
        from_def = change.get("from", {}) if isinstance(change.get("from"), dict) else {}
        to_def = change.get("to", {}) if isinstance(change.get("to"), dict) else {}
        from_types = _normalize_types(from_def)
        to_types = _normalize_types(to_def)

        if "array" in to_types and "array" not in from_types and to_types == {"array"}:
            steps.append({"action": "wrap_in_array", "field": field, "details": {"from": from_def, "to": to_def}})
            continue
        if "array" in from_types and "array" not in to_types and from_types == {"array"}:
            steps.append({"action": "unwrap_array", "field": field, "details": {"from": from_def, "to": to_def}})
            continue
        if from_types and to_types and to_types.issuperset(from_types) and from_types != to_types:
            steps.append({"action": "expand_type", "field": field, "details": {"from": from_def, "to": to_def}})
            continue
        if from_types and to_types and from_types.issuperset(to_types) and from_types != to_types and len(to_types) == 1:
            steps.append({"action": "convert_type", "field": field, "details": {"from": from_def, "to": to_def}})
            continue

        if _array_items_changed(from_def, to_def):
            steps.append({
                "action": "convert_array_items",
                "field": field,
                "details": {"from": from_def, "to": to_def},
            })
            continue

        if len(to_types) == 1:
            steps.append({"action": "convert_type", "field": field, "details": {"from": from_def, "to": to_def}})
            continue

        steps.append({"action": "review_type_change", "field": field, "details": {"from": from_def, "to": to_def}})

    existing = {step.get("field") for step in steps}
    for field, to_def in target_props.items():
        if field in existing:
            continue
        from_def = source_props.get(field, {}) if isinstance(source_props.get(field), dict) else {}
        if _array_items_changed(from_def, to_def):
            steps.append({
                "action": "convert_array_items",
                "field": field,
                "details": {"from": from_def, "to": to_def},
            })

    # Required fields added
    for field in sorted(target_required - source_required):
        field_def = target_props.get(field, {}) if isinstance(target_props.get(field), dict) else {}
        if field_def.get("default") is not None:
            steps.append({"action": "fill_missing", "field": field, "details": {"default": field_def.get("default")}})
        else:
            steps.append({"action": "review_required", "field": field, "details": {}})

    # Nullable -> non-nullable changes
    for field, to_def in target_props.items():
        if not isinstance(to_def, dict):
            continue
        from_def = source_props.get(field, {}) if isinstance(source_props.get(field), dict) else {}
        if from_def.get("nullable") is True and to_def.get("nullable") is False:
            if to_def.get("default") is not None:
                steps.append({"action": "fill_nulls", "field": field, "details": {"default": to_def.get("default")}})
            else:
                steps.append({"action": "review_nulls", "field": field, "details": {}})
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

        if action == "fill_missing":
            default = _get_default_value(properties, field)
            if default is None:
                step_results.append({"action": action, "field": field, "skipped": True})
                summary["skipped"] += 1
                continue
            updated, last_id = await _batched_update(
                coll,
                {"$or": [{field: {"$exists": False}}, {field: None}]},
                {"$set": {field: default}},
                batch_size,
                dry_run,
                rate_limit_ms,
                resume_value,
            )
            step_results.append({"action": action, "field": field, "updated": updated, "last_id": last_id})
            summary["updated"] += updated
            continue

        if action == "fill_nulls":
            default = _get_default_value(properties, field)
            if default is None:
                step_results.append({"action": action, "field": field, "skipped": True})
                summary["skipped"] += 1
                continue
            updated, last_id = await _batched_update(
                coll,
                {field: None},
                {"$set": {field: default}},
                batch_size,
                dry_run,
                rate_limit_ms,
                resume_value,
            )
            step_results.append({"action": action, "field": field, "updated": updated, "last_id": last_id})
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
            to_type = _primary_bson_type(to_type)
            if not to_type:
                step_results.append({"action": action, "field": field, "skipped": True})
                summary["skipped"] += 1
                continue
            if to_type == "null":
                updated, last_id = await _batched_update(
                    coll,
                    {field: {"$exists": True}},
                    {"$set": {field: None}},
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

        if action == "wrap_in_array":
            updated, last_id = await _batched_update(
                coll,
                {field: {"$exists": True}},
                _wrap_in_array_pipeline(field),
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

        if action == "unwrap_array":
            updated, last_id = await _batched_update(
                coll,
                {field: {"$exists": True}},
                _unwrap_array_pipeline(field),
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

        if action == "convert_array_items":
            to_item = _primary_bson_type(_get_items_bson_type(properties.get(field, {})))
            if not to_item:
                step_results.append({"action": action, "field": field, "skipped": True})
                summary["skipped"] += 1
                continue
            updated, last_id = await _batched_update(
                coll,
                {field: {"$exists": True}},
                _array_items_convert_pipeline(field, to_item),
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

        if action in {"expand_type", "review_type_change", "review_required", "review_nulls"}:
            step_results.append({"action": action, "field": field, "skipped": True})
            summary["skipped"] += 1
            continue

        step_results.append({"action": action, "field": field, "skipped": True})
        summary["skipped"] += 1

    return {"summary": summary, "steps": step_results, "dry_run": dry_run}


def _get_default(properties: Dict[str, Any], field: str) -> Any:
    field_def = properties.get(field)
    if isinstance(field_def, dict):
        return field_def.get("default")
    return None

def _get_default_value(properties: Dict[str, Any], field: str) -> Any:
    field_def = properties.get(field)
    if isinstance(field_def, dict) and field_def.get("default") is not None:
        return field_def.get("default")
    bson_type = _get_bson_type(properties, field)
    bson_type = _primary_bson_type(bson_type)
    if not bson_type:
        return None
    return _default_value_for_bson_type(bson_type)


def _default_value_for_bson_type(bson_type: str) -> Any:
    defaults = {
        "string": "",
        "int": 0,
        "double": 0.0,
        "bool": False,
        "array": [],
        "object": {},
        "date": datetime.utcnow(),
        "null": None,
    }
    return defaults.get(bson_type)


def _get_bson_type(properties: Dict[str, Any], field: str) -> Any:
    field_def = properties.get(field)
    if isinstance(field_def, dict):
        return field_def.get("bsonType")
    return None


async def _batched_update(
    coll,
    query: Dict[str, Any],
    update: Any,
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


def _primary_bson_type(bson_type: Any) -> str | None:
    if isinstance(bson_type, list):
        for t in bson_type:
            if isinstance(t, str):
                return t
        return None
    if isinstance(bson_type, str):
        return bson_type
    return None


def _array_items_changed(from_def: Dict[str, Any], to_def: Dict[str, Any]) -> bool:
    if not isinstance(from_def, dict) or not isinstance(to_def, dict):
        return False
    if from_def.get("bsonType") != "array" or to_def.get("bsonType") != "array":
        return False
    from_item = _get_items_bson_type(from_def)
    to_item = _get_items_bson_type(to_def)
    return bool(from_item and to_item and from_item != to_item)


def _wrap_in_array_pipeline(field: str) -> List[Dict[str, Any]]:
    return [
        {
            "$set": {
                field: {
                    "$cond": [
                        {"$isArray": f"${field}"},
                        f"${field}",
                        [f"${field}"]
                    ]
                }
            }
        }
    ]


def _unwrap_array_pipeline(field: str) -> List[Dict[str, Any]]:
    return [
        {
            "$set": {
                field: {
                    "$cond": [
                        {"$isArray": f"${field}"},
                        {"$arrayElemAt": [f"${field}", 0]},
                        f"${field}",
                    ]
                }
            }
        }
    ]


def _array_items_convert_pipeline(field: str, to_item_type: str) -> List[Dict[str, Any]]:
    mongo_type = _bson_to_mongo_convert_type(to_item_type)
    return [
        {
            "$set": {
                field: {
                    "$cond": [
                        {"$isArray": f"${field}"},
                        {
                            "$map": {
                                "input": f"${field}",
                                "as": "item",
                                "in": {
                                    "$convert": {
                                        "input": "$$item",
                                        "to": mongo_type,
                                        "onError": "$$item",
                                        "onNull": None,
                                    }
                                },
                            }
                        },
                        f"${field}",
                    ]
                }
            }
        }
    ]


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

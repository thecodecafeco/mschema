from __future__ import annotations

from typing import Any, Dict, List

from mongo_schematic.schema_io import get_schema_block


def diff_schemas(source: Dict[str, Any], target: Dict[str, Any]) -> Dict[str, Any]:
    source_schema = get_schema_block(source)
    target_schema = get_schema_block(target)

    source_props = source_schema.get("properties", {}) if isinstance(source_schema, dict) else {}
    target_props = target_schema.get("properties", {}) if isinstance(target_schema, dict) else {}

    added = sorted(set(target_props.keys()) - set(source_props.keys()))
    removed = sorted(set(source_props.keys()) - set(target_props.keys()))

    changed: List[Dict[str, Any]] = []
    for field in sorted(set(source_props.keys()) & set(target_props.keys())):
        src = source_props[field]
        tgt = target_props[field]
        if _field_signature(src) != _field_signature(tgt):
            changed.append({"field": field, "from": src, "to": tgt})

    return {
        "added_fields": added,
        "removed_fields": removed,
        "changed_fields": changed,
        "summary": {
            "added": len(added),
            "removed": len(removed),
            "changed": len(changed),
        },
    }


def _field_signature(field_def: Any) -> Any:
    if not isinstance(field_def, dict):
        return field_def
    return {
        "bsonType": field_def.get("bsonType"),
        "nullable": field_def.get("nullable"),
        "presence": field_def.get("presence"),
    }

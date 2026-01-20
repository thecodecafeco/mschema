"""Drift detection with severity scoring and detailed metrics."""

from __future__ import annotations

from typing import Any, Dict, List

from mongo_schematic.diff import diff_schemas


def detect_drift(
    expected_schema: Dict[str, Any],
    observed_schema: Dict[str, Any],
) -> Dict[str, Any]:
    """Detect schema drift with severity scoring.

    Args:
        expected_schema: The expected/baseline schema.
        observed_schema: The currently observed schema from live data.

    Returns:
        A dict containing diff results plus severity scoring and metrics.
    """
    diff = diff_schemas(expected_schema, observed_schema)

    severity_items = _classify_severity(diff)
    drift_score = _calculate_drift_score(diff)

    return {
        **diff,
        "severity": severity_items,
        "drift_score": drift_score,
        "has_drift": drift_score > 0,
        "critical_count": len([s for s in severity_items if s["level"] == "critical"]),
        "warning_count": len([s for s in severity_items if s["level"] == "warning"]),
        "info_count": len([s for s in severity_items if s["level"] == "info"]),
    }


def _classify_severity(diff: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Classify each drift item by severity level.

    - critical: Required field removed, type change on high-presence field
    - warning: Field removed (non-required), type changes
    - info: Field added, minor presence changes
    """
    items: List[Dict[str, Any]] = []

    for field in diff.get("added_fields", []):
        items.append({
            "level": "info",
            "type": "field_added",
            "field": field,
            "message": f"New field '{field}' detected in live data",
        })

    for field in diff.get("removed_fields", []):
        items.append({
            "level": "warning",
            "type": "field_removed",
            "field": field,
            "message": f"Field '{field}' missing from live data",
        })

    for change in diff.get("changed_fields", []):
        field = change.get("field", "unknown")
        from_def = change.get("from", {})
        to_def = change.get("to", {})

        from_type = from_def.get("bsonType") if isinstance(from_def, dict) else None
        to_type = to_def.get("bsonType") if isinstance(to_def, dict) else None

        if from_type and to_type and from_type != to_type:
            items.append({
                "level": "critical",
                "type": "type_changed",
                "field": field,
                "message": f"Type changed for '{field}': {from_type} -> {to_type}",
                "from_type": from_type,
                "to_type": to_type,
            })
        else:
            from_presence = from_def.get("presence", 0) if isinstance(from_def, dict) else 0
            to_presence = to_def.get("presence", 0) if isinstance(to_def, dict) else 0
            delta = abs(to_presence - from_presence)

            if delta > 0.2:
                level = "warning"
            else:
                level = "info"

            items.append({
                "level": level,
                "type": "field_changed",
                "field": field,
                "message": f"Field '{field}' definition changed",
                "presence_delta": round(delta, 4),
            })

    return items


def _calculate_drift_score(diff: Dict[str, Any]) -> float:
    """Calculate an overall drift score (0.0 to 1.0+).

    Scoring:
    - Each added field: +0.05
    - Each removed field: +0.15
    - Each type change: +0.25
    - Each other change: +0.1
    """
    score = 0.0

    score += len(diff.get("added_fields", [])) * 0.05
    score += len(diff.get("removed_fields", [])) * 0.15

    for change in diff.get("changed_fields", []):
        from_def = change.get("from", {})
        to_def = change.get("to", {})

        from_type = from_def.get("bsonType") if isinstance(from_def, dict) else None
        to_type = to_def.get("bsonType") if isinstance(to_def, dict) else None

        if from_type and to_type and from_type != to_type:
            score += 0.25
        else:
            score += 0.1

    return round(score, 2)

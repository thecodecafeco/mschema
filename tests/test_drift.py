"""Tests for drift detection functionality."""

from __future__ import annotations

import pytest

from mongo_schematic.drift import detect_drift, _classify_severity, _calculate_drift_score


class TestDetectDrift:
    """Tests for detect_drift function."""

    def test_no_drift(self):
        """No drift between identical schemas."""
        schema = {
            "schema": {
                "properties": {
                    "name": {"bsonType": "string", "presence": 0.95, "nullable": False},
                }
            }
        }
        result = detect_drift(schema, schema)

        assert result["has_drift"] is False
        assert result["drift_score"] == 0
        assert result["critical_count"] == 0
        assert result["warning_count"] == 0

    def test_drift_with_added_field(self):
        """Drift detected when field is added."""
        expected = {
            "schema": {"properties": {"name": {"bsonType": "string"}}}
        }
        observed = {
            "schema": {"properties": {
                "name": {"bsonType": "string"},
                "new_field": {"bsonType": "string"},
            }}
        }
        result = detect_drift(expected, observed)

        assert result["has_drift"] is True
        assert result["drift_score"] > 0
        assert result["info_count"] >= 1

    def test_drift_with_removed_field(self):
        """Drift detected when field is removed."""
        expected = {
            "schema": {"properties": {
                "name": {"bsonType": "string"},
                "old_field": {"bsonType": "string"},
            }}
        }
        observed = {
            "schema": {"properties": {"name": {"bsonType": "string"}}}
        }
        result = detect_drift(expected, observed)

        assert result["has_drift"] is True
        assert result["warning_count"] >= 1

    def test_critical_drift_type_change(self):
        """Type changes should be classified as critical."""
        expected = {
            "schema": {"properties": {"age": {"bsonType": "string", "presence": 0.9, "nullable": False}}}
        }
        observed = {
            "schema": {"properties": {"age": {"bsonType": "int", "presence": 0.9, "nullable": False}}}
        }
        result = detect_drift(expected, observed)

        assert result["has_drift"] is True
        assert result["critical_count"] >= 1


class TestClassifySeverity:
    """Tests for _classify_severity helper."""

    def test_added_field_is_info(self):
        """Added fields should be classified as info."""
        diff = {"added_fields": ["new_field"], "removed_fields": [], "changed_fields": []}
        items = _classify_severity(diff)

        assert len(items) == 1
        assert items[0]["level"] == "info"
        assert items[0]["type"] == "field_added"

    def test_removed_field_is_warning(self):
        """Removed fields should be classified as warning."""
        diff = {"added_fields": [], "removed_fields": ["old_field"], "changed_fields": []}
        items = _classify_severity(diff)

        assert len(items) == 1
        assert items[0]["level"] == "warning"
        assert items[0]["type"] == "field_removed"

    def test_type_change_is_critical(self):
        """Type changes should be classified as critical."""
        diff = {
            "added_fields": [],
            "removed_fields": [],
            "changed_fields": [{
                "field": "age",
                "from": {"bsonType": "string"},
                "to": {"bsonType": "int"},
            }],
        }
        items = _classify_severity(diff)

        assert len(items) == 1
        assert items[0]["level"] == "critical"
        assert items[0]["type"] == "type_changed"


class TestCalculateDriftScore:
    """Tests for _calculate_drift_score helper."""

    def test_zero_score_no_changes(self):
        """No changes should produce zero score."""
        diff = {"added_fields": [], "removed_fields": [], "changed_fields": []}
        score = _calculate_drift_score(diff)
        assert score == 0.0

    def test_added_fields_contribute_05(self):
        """Each added field contributes 0.05 to score."""
        diff = {"added_fields": ["a", "b"], "removed_fields": [], "changed_fields": []}
        score = _calculate_drift_score(diff)
        assert score == 0.1

    def test_removed_fields_contribute_15(self):
        """Each removed field contributes 0.15 to score."""
        diff = {"added_fields": [], "removed_fields": ["a"], "changed_fields": []}
        score = _calculate_drift_score(diff)
        assert score == 0.15

    def test_type_change_contributes_25(self):
        """Type changes contribute 0.25 to score."""
        diff = {
            "added_fields": [],
            "removed_fields": [],
            "changed_fields": [{"from": {"bsonType": "string"}, "to": {"bsonType": "int"}}],
        }
        score = _calculate_drift_score(diff)
        assert score == 0.25

"""Tests for schema diff functionality."""

from __future__ import annotations

import pytest

from mongo_schematic.diff import diff_schemas, _field_signature


class TestDiffSchemas:
    """Tests for diff_schemas function."""

    def test_no_diff_identical_schemas(self):
        """Identical schemas should produce no diff."""
        schema = {
            "schema": {
                "type": "object",
                "properties": {
                    "name": {"bsonType": "string", "presence": 0.95, "nullable": False},
                    "age": {"bsonType": "int", "presence": 0.90, "nullable": False},
                },
                "required": ["name"],
            }
        }
        result = diff_schemas(schema, schema)

        assert result["added_fields"] == []
        assert result["removed_fields"] == []
        assert result["changed_fields"] == []
        assert result["summary"]["added"] == 0
        assert result["summary"]["removed"] == 0
        assert result["summary"]["changed"] == 0

    def test_added_field(self):
        """Detect added fields between schemas."""
        source = {
            "schema": {
                "properties": {
                    "name": {"bsonType": "string"},
                }
            }
        }
        target = {
            "schema": {
                "properties": {
                    "name": {"bsonType": "string"},
                    "email": {"bsonType": "string"},
                }
            }
        }
        result = diff_schemas(source, target)

        assert "email" in result["added_fields"]
        assert result["summary"]["added"] == 1

    def test_removed_field(self):
        """Detect removed fields between schemas."""
        source = {
            "schema": {
                "properties": {
                    "name": {"bsonType": "string"},
                    "legacy_field": {"bsonType": "string"},
                }
            }
        }
        target = {
            "schema": {
                "properties": {
                    "name": {"bsonType": "string"},
                }
            }
        }
        result = diff_schemas(source, target)

        assert "legacy_field" in result["removed_fields"]
        assert result["summary"]["removed"] == 1

    def test_changed_field_type(self):
        """Detect type changes in fields."""
        source = {
            "schema": {
                "properties": {
                    "age": {"bsonType": "string", "presence": 0.9, "nullable": False},
                }
            }
        }
        target = {
            "schema": {
                "properties": {
                    "age": {"bsonType": "int", "presence": 0.9, "nullable": False},
                }
            }
        }
        result = diff_schemas(source, target)

        assert len(result["changed_fields"]) == 1
        assert result["changed_fields"][0]["field"] == "age"
        assert result["summary"]["changed"] == 1

    def test_empty_schemas(self):
        """Handle empty schemas gracefully."""
        result = diff_schemas({}, {})

        assert result["added_fields"] == []
        assert result["removed_fields"] == []
        assert result["changed_fields"] == []

    def test_schema_with_nested_structure(self):
        """Schema with 'schema' key should be handled correctly."""
        source = {"schema": {"properties": {"a": {"bsonType": "string"}}}}
        target = {"schema": {"properties": {"b": {"bsonType": "string"}}}}
        result = diff_schemas(source, target)

        assert "b" in result["added_fields"]
        assert "a" in result["removed_fields"]


class TestFieldSignature:
    """Tests for _field_signature helper."""

    def test_dict_field(self):
        """Dict fields should extract signature keys."""
        field = {"bsonType": "string", "nullable": True, "presence": 0.5, "extra": "ignored"}
        sig = _field_signature(field)

        assert sig["bsonType"] == "string"
        assert sig["nullable"] is True
        assert sig["presence"] == 0.5
        assert "extra" not in sig

    def test_non_dict_field(self):
        """Non-dict fields should be returned as-is."""
        assert _field_signature("string") == "string"
        assert _field_signature(123) == 123

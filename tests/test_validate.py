"""Tests for validation functionality."""

from __future__ import annotations

import pytest
from datetime import datetime
from decimal import Decimal

from mongo_schematic.validate import build_mongo_validator, _validate_document


class TestBuildMongoValidator:
    """Tests for build_mongo_validator function."""

    def test_basic_validator_structure(self):
        """Validator should have correct JSON schema structure."""
        schema = {
            "schema": {
                "properties": {
                    "name": {"bsonType": "string"},
                    "age": {"bsonType": "int"},
                },
                "required": ["name"],
            }
        }
        result = build_mongo_validator(schema)

        assert "$jsonSchema" in result
        assert result["$jsonSchema"]["bsonType"] == "object"
        assert result["$jsonSchema"]["required"] == ["name"]
        assert "name" in result["$jsonSchema"]["properties"]
        assert "age" in result["$jsonSchema"]["properties"]

    def test_empty_schema(self):
        """Empty schema should produce minimal validator."""
        result = build_mongo_validator({})

        assert "$jsonSchema" in result
        assert result["$jsonSchema"]["required"] == []
        assert result["$jsonSchema"]["properties"] == {}

    def test_properties_only_include_bson_type(self):
        """Validator properties should only include bsonType."""
        schema = {
            "schema": {
                "properties": {
                    "email": {
                        "bsonType": "string",
                        "presence": 0.95,
                        "nullable": False,
                    },
                },
                "required": [],
            }
        }
        result = build_mongo_validator(schema)

        email_prop = result["$jsonSchema"]["properties"]["email"]
        assert email_prop == {"bsonType": "string"}
        assert "presence" not in email_prop
        assert "nullable" not in email_prop


class TestValidateDocument:
    """Tests for _validate_document helper."""

    def test_valid_document(self):
        """Valid document should produce no issues."""
        doc = {"name": "John", "age": 30}
        properties = {
            "name": {"bsonType": "string"},
            "age": {"bsonType": "int"},
        }
        required = {"name"}

        issues = _validate_document(doc, properties, required)
        assert issues == []

    def test_missing_required_field(self):
        """Missing required field should produce an issue."""
        doc = {"age": 30}
        properties = {
            "name": {"bsonType": "string"},
            "age": {"bsonType": "int"},
        }
        required = {"name"}

        issues = _validate_document(doc, properties, required)
        assert len(issues) == 1
        assert "Missing required field: name" in issues[0]

    def test_null_required_field(self):
        """Null required field should produce an issue."""
        doc = {"name": None, "age": 30}
        properties = {
            "name": {"bsonType": "string"},
            "age": {"bsonType": "int"},
        }
        required = {"name"}

        issues = _validate_document(doc, properties, required)
        assert len(issues) == 1
        assert "Missing required field: name" in issues[0]

    def test_type_mismatch(self):
        """Wrong type should produce an issue."""
        doc = {"name": "John", "age": "thirty"}
        properties = {
            "name": {"bsonType": "string"},
            "age": {"bsonType": "int"},
        }
        required = set()

        issues = _validate_document(doc, properties, required)
        assert len(issues) == 1
        assert "Type mismatch for age" in issues[0]

    def test_optional_field_missing_is_ok(self):
        """Optional fields can be missing without issue."""
        doc = {"name": "John"}
        properties = {
            "name": {"bsonType": "string"},
            "nickname": {"bsonType": "string"},
        }
        required = {"name"}

        issues = _validate_document(doc, properties, required)
        assert issues == []

    def test_double_accepts_int_and_float(self):
        """Double type should accept both int and float."""
        doc_int = {"value": 42}
        doc_float = {"value": 42.5}
        properties = {"value": {"bsonType": "double"}}
        required = set()

        assert _validate_document(doc_int, properties, required) == []
        assert _validate_document(doc_float, properties, required) == []

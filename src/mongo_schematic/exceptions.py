"""Custom exceptions for MongoSchematic CLI."""

from __future__ import annotations


class MongoSchematicError(Exception):
    """Base exception for MongoSchematic."""

    pass


class ConfigurationError(MongoSchematicError):
    """Raised when configuration is missing or invalid."""

    pass


class ConnectionError(MongoSchematicError):
    """Raised when MongoDB connection fails."""

    pass


class ValidationError(MongoSchematicError):
    """Raised when schema validation fails."""

    pass


class AIError(MongoSchematicError):
    """Raised when Gemini AI operations fail."""

    pass

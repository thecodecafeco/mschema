"""Tests for configuration loading."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from mongo_schematic.config import (
    FileConfig,
    EnvConfig,
    load_file_config,
    load_runtime_config,
    write_default_config,
)
from mongo_schematic.exceptions import ConfigurationError


class TestFileConfig:
    """Tests for FileConfig model."""

    def test_all_optional(self):
        """All fields should be optional."""
        config = FileConfig()
        assert config.mongodb_uri is None
        assert config.default_db is None
        assert config.gemini_api_key is None

    def test_with_values(self):
        """Fields should accept values."""
        config = FileConfig(
            mongodb_uri="mongodb://localhost",
            default_db="test",
            gemini_api_key="key123",
        )
        assert config.mongodb_uri == "mongodb://localhost"
        assert config.default_db == "test"
        assert config.gemini_api_key == "key123"


class TestLoadFileConfig:
    """Tests for load_file_config function."""

    def test_missing_file_returns_empty(self):
        """Missing file should return empty config."""
        config = load_file_config(Path("/nonexistent/.mongo_schematic.yml"))
        assert config.mongodb_uri is None
        assert config.default_db is None

    def test_valid_yaml_file(self):
        """Valid YAML file should be parsed."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml.dump({
                "mongodb_uri": "mongodb://test:27017",
                "default_db": "mydb",
            }, f)
            f.flush()

            try:
                config = load_file_config(Path(f.name))
                assert config.mongodb_uri == "mongodb://test:27017"
                assert config.default_db == "mydb"
            finally:
                os.unlink(f.name)

    def test_empty_yaml_file(self):
        """Empty YAML file should return empty config."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("")
            f.flush()

            try:
                config = load_file_config(Path(f.name))
                assert config.mongodb_uri is None
            finally:
                os.unlink(f.name)


class TestLoadRuntimeConfig:
    """Tests for load_runtime_config function."""

    def test_missing_uri_raises_error(self, monkeypatch):
        """Missing MongoDB URI should raise ConfigurationError."""
        monkeypatch.delenv("MSCHEMA_MONGODB_URI", raising=False)
        monkeypatch.delenv("MSCHEMA_DEFAULT_DB", raising=False)

        with pytest.raises(ConfigurationError) as exc_info:
            load_runtime_config(Path("/nonexistent/.mongo_schematic.yml"))

        assert "Missing MongoDB URI" in str(exc_info.value)

    def test_missing_db_raises_error(self, monkeypatch):
        """Missing default DB should raise ConfigurationError."""
        monkeypatch.setenv("MSCHEMA_MONGODB_URI", "mongodb://localhost")
        monkeypatch.delenv("MSCHEMA_DEFAULT_DB", raising=False)

        with pytest.raises(ConfigurationError) as exc_info:
            load_runtime_config(Path("/nonexistent/.mongo_schematic.yml"))

        assert "Missing default DB" in str(exc_info.value)

    def test_env_vars_override_file(self, monkeypatch):
        """Environment variables should override file config."""
        monkeypatch.setenv("MSCHEMA_MONGODB_URI", "mongodb://env:27017")
        monkeypatch.setenv("MSCHEMA_DEFAULT_DB", "envdb")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml.dump({
                "mongodb_uri": "mongodb://file:27017",
                "default_db": "filedb",
            }, f)
            f.flush()

            try:
                config = load_runtime_config(Path(f.name))
                assert config.mongodb_uri == "mongodb://env:27017"
                assert config.default_db == "envdb"
            finally:
                os.unlink(f.name)


class TestWriteDefaultConfig:
    """Tests for write_default_config function."""

    def test_creates_config_file(self):
        """Should create config file if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / ".mongo_schematic.yml"
            result = write_default_config(path)

            assert result == path
            assert path.exists()

            content = yaml.safe_load(path.read_text())
            assert "mongodb_uri" in content
            assert "default_db" in content

    def test_does_not_overwrite_existing(self):
        """Should not overwrite existing config file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("custom: value\n")
            f.flush()

            try:
                result = write_default_config(Path(f.name))
                content = Path(f.name).read_text()
                assert "custom: value" in content
            finally:
                os.unlink(f.name)

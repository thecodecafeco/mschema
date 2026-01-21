from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from mongo_schematic.exceptions import ConfigurationError


class EnvConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MSCHEMA_", case_sensitive=False)

    mongodb_uri: Optional[str] = None
    default_db: Optional[str] = None
    gemini_api_key: Optional[str] = None


class FileConfig(BaseModel):
    mongodb_uri: Optional[str] = None
    default_db: Optional[str] = None
    gemini_api_key: Optional[str] = None


class RuntimeConfig(BaseModel):
    mongodb_uri: str = Field(..., description="MongoDB connection string")
    default_db: str = Field(..., description="Default database")
    gemini_api_key: Optional[str] = None


DEFAULT_CONFIG_PATH = Path.cwd() / ".mschema.yml"
LOCAL_CONFIG_PATH = Path.cwd() / ".mschema.local.yml"


def load_file_config(path: Path = DEFAULT_CONFIG_PATH) -> FileConfig:
    if not path.exists():
        return FileConfig()

    data = yaml.safe_load(path.read_text()) or {}
    return FileConfig(**data)


def load_runtime_config(path: Path = DEFAULT_CONFIG_PATH) -> RuntimeConfig:
    """Load configuration with priority: env vars > local file > main file."""
    # Load main config file
    file_config = load_file_config(path)
    
    # Load local override file (gitignored, for safe local testing)
    local_path = path.parent / ".mschema.local.yml" if path != DEFAULT_CONFIG_PATH else LOCAL_CONFIG_PATH
    local_config = load_file_config(local_path)
    
    # Load environment variables
    env_config = EnvConfig()

    # Priority: env > local > file
    mongodb_uri = env_config.mongodb_uri or local_config.mongodb_uri or file_config.mongodb_uri
    default_db = env_config.default_db or local_config.default_db or file_config.default_db
    gemini_api_key = env_config.gemini_api_key or local_config.gemini_api_key or file_config.gemini_api_key

    if not mongodb_uri:
        raise ConfigurationError("Missing MongoDB URI. Set in .mschema.yml, .mschema.local.yml, or MSCHEMA_MONGODB_URI.")
    if not default_db:
        raise ConfigurationError("Missing default DB. Set in .mschema.yml, .mschema.local.yml, or MSCHEMA_DEFAULT_DB.")

    return RuntimeConfig(
        mongodb_uri=mongodb_uri,
        default_db=default_db,
        gemini_api_key=gemini_api_key,
    )


def write_default_config(path: Path = DEFAULT_CONFIG_PATH) -> Path:
    if path.exists():
        return path

    content = {
        "mongodb_uri": "mongodb://localhost:27017",
        "default_db": "myapp",
        "gemini_api_key": "",
    }
    path.write_text(yaml.safe_dump(content, sort_keys=False))
    return path

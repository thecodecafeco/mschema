from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from beanie import Document
from pydantic import Field


class SchemaSnapshot(Document):
    database: str
    collection: str
    schema_payload: Dict[str, Any] = Field(alias="schema")
    confidence: float
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "schema_snapshots"


class AnalysisRun(Document):
    database: str
    collection: str
    sample_size: int
    total_documents: int
    anomalies: list[Dict[str, Any]]
    recommendations: list[Dict[str, Any]]
    schema_snapshot_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "analysis_runs"

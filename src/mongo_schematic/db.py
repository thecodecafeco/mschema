from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie

from mongo_schematic.models import SchemaSnapshot, AnalysisRun


def get_motor_client(mongodb_uri: str) -> AsyncIOMotorClient:
    return AsyncIOMotorClient(mongodb_uri)


async def init_odm(mongodb_uri: str, database: str) -> AsyncIOMotorClient:
    client = AsyncIOMotorClient(mongodb_uri)
    await init_beanie(
        database=client[database],
        document_models=[SchemaSnapshot, AnalysisRun],
    )
    return client

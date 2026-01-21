import random
from datetime import datetime
from typing import Any, Dict, List, Optional

from bson import Binary, Code, Int64, MaxKey, MinKey, Regex, Timestamp
from faker import Faker
from motor.motor_asyncio import AsyncIOMotorClient

fake = Faker()

def _generate_value_for_field(field_name: str, field_def: Dict[str, Any]) -> Any:
    """Generate a fake value based on field name and BSON type."""
    bson_type = field_def.get("bsonType", "string")
    field_lower = field_name.lower()
    
    # specialized generators based on name
    if "email" in field_lower and bson_type == "string":
        return fake.email()
    if "first_name" in field_lower:
        return fake.first_name()
    if "last_name" in field_lower:
        return fake.last_name()
    if "name" in field_lower:
        return fake.name()
    if "phone" in field_lower:
        return fake.phone_number()
    if "address" in field_lower:
        return fake.address()
    if "city" in field_lower:
        return fake.city()
    if "country" in field_lower:
        return fake.country()
    if "company" in field_lower:
        return fake.company()
    if "date" in field_lower or "created_at" in field_lower:
        return datetime.utcnow()
    if "id" in field_lower and bson_type == "string":
        return fake.uuid4()
    if "url" in field_lower or "website" in field_lower:
        return fake.url()
    
    # generic generators based on type
    if bson_type == "string":
        return fake.word()
    if bson_type == "int":
        return random.randint(0, 1000)
    if bson_type == "double" or bson_type == "decimal":
        return round(random.uniform(0.0, 1000.0), 2)
    if bson_type == "bool":
        return fake.boolean()
    if bson_type == "date":
        return datetime.utcnow()
    if bson_type == "objectId":
        return fake.hexify(text="^" * 24)
    if bson_type == "array":
        # Generate simple array of 1-3 items
        return [_generate_value_for_field(f"{field_name}_item", {"bsonType": "string"}) for _ in range(random.randint(1, 3))]
    if bson_type == "object":
        return {} # Simplified
    
    # New BSON types
    if bson_type == "long":
        return Int64(random.randint(0, 1000000))
    if bson_type == "binData":
        return Binary(fake.binary(length=16))
    if bson_type == "regex":
        return Regex(r"^[a-z]+$", "i")
    if bson_type == "timestamp":
        return Timestamp(int(datetime.utcnow().timestamp()), 1)
    if bson_type == "minKey":
        return MinKey()
    if bson_type == "maxKey":
        return MaxKey()
    if bson_type == "javascript":
        return Code("function() { return true; }")
    if bson_type == "null":
        return None
    
    return None

def generate_document(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a single fake document from schema."""
    doc = {}
    properties = schema.get("schema", schema).get("properties", {})
    required = set(schema.get("schema", schema).get("required", []))
    
    for field, field_def in properties.items():
        # Skip some optional fields to mimic real data
        if field not in required and random.random() < 0.3:
            continue
            
        doc[field] = _generate_value_for_field(field, field_def)
    
    return doc

async def seed_collection(
    client: AsyncIOMotorClient,
    db_name: str,
    coll_name: str,
    schema: Dict[str, Any],
    count: int
) -> int:
    """Seed collection with fake data."""
    coll = client[db_name][coll_name]
    docs = [generate_document(schema) for _ in range(count)]
    
    if docs:
        result = await coll.insert_many(docs)
        return len(result.inserted_ids)
    return 0

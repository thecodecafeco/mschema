from typing import Any, Dict, List, Set, Tuple

def _get_ts_type(bson_type: str) -> str:
    """Map BSON type to TypeScript type."""
    mapping = {
        "string": "string",
        "int": "number",
        "double": "number",
        "bool": "boolean",
        "objectId": "string",
        "date": "Date",
        "array": "any[]",
        "object": "any",
        "null": "null",
        "long": "number",
        "decimal": "number",
        "binData": "Buffer",
        "regex": "RegExp",
        "timestamp": "Date",
        "minKey": "any",
        "maxKey": "any",
        "javascript": "string",
        "dbPointer": "string",
    }
    return mapping.get(bson_type, "any")

def generate_typescript_code(schema: Dict[str, Any], interface_name: str = "Interface") -> str:
    """Generate TypeScript interfaces from schema."""
    interfaces_to_generate: List[Tuple[str, Dict[str, Any]]] = [(interface_name, schema)]
    generated_interfaces: Set[str] = set()
    interface_definitions: List[str] = []

    while interfaces_to_generate:
        curr_name, curr_schema = interfaces_to_generate.pop(0)
        if curr_name in generated_interfaces:
            continue
        
        generated_interfaces.add(curr_name)
        schema_props = curr_schema.get("schema", curr_schema).get("properties", {})
        required_fields = set(curr_schema.get("schema", curr_schema).get("required", []))
        
        lines = [f"export interface {curr_name} {{"]
        
        for field_name, field_def in schema_props.items():
            bson_type = field_def.get("bsonType", "any")
            
            # Handle union types (list of bsonTypes)
            if isinstance(bson_type, list):
                # Generate union type for multiple types
                type_list = [_get_ts_type(t) for t in bson_type]
                ts_type = " | ".join(type_list)
            # Handle nested objects
            elif bson_type == "object":
                # Check inner properties if available (current schema structure permitting)
                 if "properties" in field_def:
                    nested_name = f"{curr_name}{field_name.capitalize()}"
                    ts_type = nested_name
                    interfaces_to_generate.append((nested_name, field_def))
                 else:
                    ts_type = "any" # Record<string, any> ?
            elif bson_type == "array":
                ts_type = "any[]"
            else:
                ts_type = _get_ts_type(bson_type)
            
            is_required = field_name in required_fields
            nullable = field_def.get("nullable", False)
            
            optional_mark = "?" if not is_required else ""
            
            if nullable:
                ts_type = f"{ts_type} | null"
            
            lines.append(f"  {field_name}{optional_mark}: {ts_type};")
            
        lines.append("}")
        interface_definitions.insert(0, "\n".join(lines))

    return "\n\n".join(interface_definitions)

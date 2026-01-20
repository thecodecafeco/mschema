from typing import Any, Dict, List, Set, Tuple

def _sanitize_name(name: str) -> str:
    """Sanitize field name for Python."""
    # Simple sanitization, can be expanded
    return name.replace(" ", "_").replace("-", "_")

def _to_pascal_case(name: str) -> str:
    """Convert snake_case to PascalCase."""
    return "".join(word.capitalize() for word in _sanitize_name(name).split("_"))

def _get_python_type(bson_type: str) -> str:
    """Map BSON type to Python type."""
    mapping = {
        "string": "str",
        "int": "int",
        "double": "float",
        "bool": "bool",
        "objectId": "str",  # Often represented as str in Pydantic models for APIs
        "date": "datetime",
        "array": "List",
        "object": "Dict[str, Any]",
        "null": "Any",
        "long": "int",
        "decimal": "Decimal",
    }
    return mapping.get(bson_type, "Any")

def generate_pydantic_code(schema: Dict[str, Any], class_name: str = "Model") -> str:
    """Generate Pydantic model code from schema."""
    lines = [
        "from typing import Any, Dict, List, Optional",
        "from datetime import datetime",
        "from decimal import Decimal",
        "from pydantic import BaseModel, Field",
        "",
        ""
    ]
    
    models_to_generate: List[Tuple[str, Dict[str, Any]]] = [(class_name, schema)]
    generated_models: Set[str] = set()
    model_definitions: List[str] = []

    while models_to_generate:
        curr_name, curr_schema = models_to_generate.pop(0)
        if curr_name in generated_models:
            continue
        
        generated_models.add(curr_name)
        schema_props = curr_schema.get("schema", curr_schema).get("properties", {})
        required_fields = set(curr_schema.get("schema", curr_schema).get("required", []))
        
        model_lines = [f"class {curr_name}(BaseModel):"]
        
        if not schema_props:
            model_lines.append("    pass")
            model_definitions.append("\n".join(model_lines))
            continue

        for field_name, field_def in schema_props.items():
            safe_name = _sanitize_name(field_name)
            bson_type = field_def.get("bsonType", "any")
            
            # Handle nested objects
            if bson_type == "object":
                nested_name = f"{curr_name}{_to_pascal_case(field_name)}"
                python_type = nested_name
                # Enqueue nested model for generation, but we need the sub-schema
                # Ideally the schema analysis would provide nested structure deeper than currently implemented
                # For now, treat unknown nested structure as Dict if not fully defined
                # The current analyzer might not return deep nested structures in 'properties' 
                # effectively unless it's a recursive recursive call.
                # Assuming simple flat properties for now based on current analyze implementation
                # If properties has 'properties', it's nested.
                if "properties" in field_def:
                     models_to_generate.append((nested_name, field_def))
                else:
                    python_type = "Dict[str, Any]"

            elif bson_type == "array":
                # Very basic array handling
                python_type = "List[Any]"
                # check if array of objects? (Not fully captured in current simplified analysis)
            
            else:
                python_type = _get_python_type(bson_type)

            is_required = field_name in required_fields
            nullable = field_def.get("nullable", False)
            
            if not is_required or nullable:
                type_hint = f"Optional[{python_type}]"
                default = " = None"
            else:
                type_hint = python_type
                default = ""
            
            if safe_name != field_name:
                field_arg = f', alias="{field_name}"'
                default = f' = Field(default=None{field_arg})' if not is_required else f' = Field(..., alias="{field_name}")'
            
            model_lines.append(f"    {safe_name}: {type_hint}{default}")

        model_definitions.insert(0, "\n".join(model_lines)) # Prepend to handle dependencies (basic)
        
    lines.extend(model_definitions)
    return "\n\n".join(lines)

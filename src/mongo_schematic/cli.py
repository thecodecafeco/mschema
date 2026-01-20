from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional
from urllib import request

import typer
from rich.console import Console

from mongo_schematic.analyze import analyze_collection
from mongo_schematic.ai import generate_migration_plan_with_gemini, generate_recommendations_with_gemini
from mongo_schematic.config import DEFAULT_CONFIG_PATH, load_runtime_config, write_default_config
from mongo_schematic.db import get_motor_client, init_odm
from mongo_schematic import __version__
from mongo_schematic.models import AnalysisRun, SchemaSnapshot
from mongo_schematic.schema_io import load_schema, write_schema
from mongo_schematic.diff import diff_schemas
from mongo_schematic.drift import detect_drift
from mongo_schematic.validate import apply_validation, validate_collection
from mongo_schematic.migrate import apply_migration_plan, generate_migration_file, generate_migration_plan
from mongo_schematic.reporting import print_json
from mongo_schematic.indexes import index_usage, list_indexes, recommend_indexes
from mongo_schematic.codegen.pydantic import generate_pydantic_code
from mongo_schematic.codegen.typescript import generate_typescript_code
from mongo_schematic.docs_gen import generate_docs
from mongo_schematic.seed import seed_collection
from mongo_schematic.hooks import install_hooks



app = typer.Typer(no_args_is_help=True)
schema_app = typer.Typer(no_args_is_help=True)
drift_app = typer.Typer(no_args_is_help=True)
validate_app = typer.Typer(no_args_is_help=True)
migrate_app = typer.Typer(no_args_is_help=True)
db_app = typer.Typer(no_args_is_help=True, help="Database-wide operations")
generate_app = typer.Typer(no_args_is_help=True, help="Code generation")
docs_app = typer.Typer(no_args_is_help=True, help="Documentation generation")
hook_app = typer.Typer(no_args_is_help=True, help="Git hook management")
app.add_typer(schema_app, name="schema")
app.add_typer(drift_app, name="drift")
app.add_typer(validate_app, name="validate")
app.add_typer(migrate_app, name="migrate")
app.add_typer(db_app, name="db")
app.add_typer(generate_app, name="generate")
app.add_typer(docs_app, name="docs")
app.add_typer(hook_app, name="hook")
console = Console()



def _post_webhook(url: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type": "application/json"})
    request.urlopen(req, timeout=10)


@app.command()
def version() -> None:
    console.print(f"MongoSchematic CLI v{__version__}")


@app.command()
def init(path: Optional[Path] = typer.Option(None, "--path", help="Path for config file")) -> None:
    config_path = path or DEFAULT_CONFIG_PATH
    if config_path.exists():
        console.print(f"Config already exists at {config_path}")
        raise typer.Exit(code=0)

    write_default_config(config_path)
    console.print(f"Created config at {config_path}")


@app.command()
def analyze(
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
    sample: int = typer.Option(10000, "--sample", help="Sample size"),
    output: str = typer.Option("json", "--output", help="Output format: json"),
    use_ai: bool = typer.Option(True, "--ai/--no-ai", help="Use Gemini for recommendations"),
    store: bool = typer.Option(False, "--store", help="Store analysis via Beanie ODM"),
    save: Optional[Path] = typer.Option(None, "--save", help="Save schema output to file"),
) -> None:
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
            gemini_key = None
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db
            gemini_key = config.gemini_api_key

        client = get_motor_client(mongodb_uri)
        result = await analyze_collection(client, default_db, collection, sample)

        if use_ai:
            gemini_recs = generate_recommendations_with_gemini(
                gemini_key, result.get("schema", {}), result.get("anomalies", [])
            )
            if gemini_recs:
                result["recommendations"].extend(gemini_recs)

        if store:
            odm_client = await init_odm(mongodb_uri, default_db)
            snapshot = SchemaSnapshot(
                database=default_db,
                collection=collection,
                schema=result.get("schema", {}),
                confidence=result.get("confidence", 0.0),
            )
            await snapshot.insert()

            run = AnalysisRun(
                database=default_db,
                collection=collection,
                sample_size=sample,
                total_documents=result.get("total_documents", 0),
                anomalies=result.get("anomalies", []),
                recommendations=result.get("recommendations", []),
                schema_snapshot_id=str(snapshot.id),
            )
            await run.insert()
            odm_client.close()

        if save:
            write_schema(save, result)

        if output == "json":
            print_json(result)
        else:
            console.print(json.dumps(result, indent=2))

        client.close()

    asyncio.run(_run())


@schema_app.command("export")
def schema_export(
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
    sample: int = typer.Option(10000, "--sample", help="Sample size"),
    out: Path = typer.Option(..., "--out", help="Output schema file path"),
    use_ai: bool = typer.Option(True, "--ai/--no-ai", help="Use Gemini for recommendations"),
) -> None:
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
            gemini_key = None
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db
            gemini_key = config.gemini_api_key

        client = get_motor_client(mongodb_uri)
        result = await analyze_collection(client, default_db, collection, sample)

        if use_ai:
            gemini_recs = generate_recommendations_with_gemini(
                gemini_key, result.get("schema", {}), result.get("anomalies", [])
            )
            if gemini_recs:
                result["recommendations"].extend(gemini_recs)

        write_schema(out, result)
        print_json({"status": "saved", "path": str(out)})
        client.close()

    asyncio.run(_run())


@schema_app.command("diff")
def schema_diff(
    source: Path = typer.Option(..., "--from", help="Source schema file"),
    target: Path = typer.Option(..., "--to", help="Target schema file"),
) -> None:
    result = diff_schemas(load_schema(source), load_schema(target))
    print_json(result)


@schema_app.command("recommend-indexes")
def schema_recommend_indexes(
    schema: Path = typer.Option(..., "--schema", help="Schema file"),
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
) -> None:
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        indexes = await list_indexes(client, default_db, collection)
        recs = recommend_indexes(load_schema(schema), indexes)
        print_json({"indexes": indexes, "recommendations": recs})
        client.close()

    asyncio.run(_run())


@schema_app.command("index-usage")
def schema_index_usage(
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
) -> None:
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        usage = await index_usage(client, default_db, collection)
        print_json({"index_usage": usage})
        client.close()

    asyncio.run(_run())


@drift_app.command("detect")
def drift_detect(
    expected: Path = typer.Option(..., "--schema", help="Expected schema file"),
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
    sample: int = typer.Option(10000, "--sample", help="Sample size"),
) -> None:
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        observed = await analyze_collection(client, default_db, collection, sample)
        expected_schema = load_schema(expected)
        result = detect_drift(expected_schema, observed)
        print_json(result)
        client.close()

        if result.get("has_drift"):
            raise typer.Exit(code=1)

    asyncio.run(_run())


@drift_app.command("compare")
def drift_compare(
    source: Path = typer.Option(..., "--from", help="Source schema file"),
    target: Path = typer.Option(..., "--to", help="Target schema file"),
) -> None:
    result = diff_schemas(load_schema(source), load_schema(target))
    print_json(result)


@drift_app.command("monitor")
def drift_monitor(
    expected: Path = typer.Option(..., "--schema", help="Expected schema file"),
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
    sample: int = typer.Option(10000, "--sample", help="Sample size"),
    interval: int = typer.Option(300, "--interval", help="Seconds between checks"),
    webhook: Optional[str] = typer.Option(None, "--webhook", help="Webhook URL for alerts"),
) -> None:
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        expected_schema = load_schema(expected)
        while True:
            client = get_motor_client(mongodb_uri)
            observed = await analyze_collection(client, default_db, collection, sample)
            result = detect_drift(expected_schema, observed)
            print_json(result)
            if webhook:
                _post_webhook(webhook, result)
            client.close()
            await asyncio.sleep(interval)

    asyncio.run(_run())


@validate_app.command("test")
def validate_test(
    schema: Path = typer.Option(..., "--schema", help="Schema file"),
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
    sample: int = typer.Option(10000, "--sample", help="Sample size"),
    max_errors: int = typer.Option(100, "--max-errors", help="Max errors to return"),
) -> None:
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        result = await validate_collection(
            client,
            default_db,
            collection,
            load_schema(schema),
            sample,
            max_errors,
        )
        print_json(result)
        client.close()

    asyncio.run(_run())


@migrate_app.command("create")
def migrate_create(
    source: Path = typer.Option(..., "--from", help="Source schema file"),
    target: Path = typer.Option(..., "--to", help="Target schema file"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
    out: Path = typer.Option(..., "--out", help="Migration file path"),
) -> None:
    path = generate_migration_file(load_schema(source), load_schema(target), collection, out)
    print_json({"status": "created", "path": str(path)})


@migrate_app.command("plan")
def migrate_plan(
    source: Path = typer.Option(..., "--from", help="Source schema file"),
    target: Path = typer.Option(..., "--to", help="Target schema file"),
    use_ai: bool = typer.Option(False, "--ai/--no-ai", help="Use Gemini to refine plan"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output plan file path"),
) -> None:
    source_schema = load_schema(source)
    target_schema = load_schema(target)
    base_plan = generate_migration_plan(source_schema, target_schema)
    if use_ai:
        config = load_runtime_config()
        ai_plan = generate_migration_plan_with_gemini(config.gemini_api_key, base_plan)
        if ai_plan:
            if out:
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text(json.dumps(ai_plan, indent=2))
                print_json({"status": "saved", "path": str(out)})
                return
            print_json(ai_plan)
            return

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(base_plan, indent=2))
        print_json({"status": "saved", "path": str(out)})
        return
    print_json(base_plan)


@migrate_app.command("apply")
def migrate_apply(
    plan: Path = typer.Option(..., "--plan", help="Migration plan JSON file"),
    to_schema: Path = typer.Option(..., "--to", help="Target schema file"),
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
    allow_remove: bool = typer.Option(False, "--allow-remove", help="Allow field removal"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Do not write changes"),
    rate_limit_ms: int = typer.Option(0, "--rate-limit-ms", help="Delay between batches"),
    resume_from: Optional[str] = typer.Option(None, "--resume-from", help="Resume from _id"),
) -> None:
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        plan_payload = json.loads(plan.read_text())
        client = get_motor_client(mongodb_uri)
        resume_value = resume_from
        result = await apply_migration_plan(
            client,
            default_db,
            collection,
            plan_payload,
            load_schema(to_schema),
            allow_remove,
            dry_run,
            rate_limit_ms,
            resume_value,
        )
        print_json(result)
        client.close()

    asyncio.run(_run())


@validate_app.command("apply")
def validate_apply(
    schema: Path = typer.Option(..., "--schema", help="Schema file"),
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
    level: str = typer.Option("moderate", "--level", help="Validation level"),
    action: str = typer.Option("error", "--action", help="Validation action"),
) -> None:
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        result = await apply_validation(
            client,
            default_db,
            collection,
            load_schema(schema),
            level,
            action,
        )
        print_json(result)
        client.close()

    asyncio.run(_run())


# =============================================================================
# Database-Wide Commands
# =============================================================================


@db_app.command("analyze")
def db_analyze(
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    sample: int = typer.Option(5000, "--sample", help="Sample size per collection"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output JSON file"),
) -> None:
    """Analyze all collections in the database."""
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        database = client[default_db]
        collection_names = await database.list_collection_names()
        
        results = {"database": default_db, "collections": {}, "summary": {"total": 0, "with_anomalies": 0}}
        
        for coll_name in sorted(collection_names):
            if coll_name.startswith("system."):
                continue
            console.print(f"[dim]Analyzing {coll_name}...[/dim]")
            result = await analyze_collection(client, default_db, coll_name, sample)
            results["collections"][coll_name] = result
            results["summary"]["total"] += 1
            if result.get("anomalies"):
                results["summary"]["with_anomalies"] += 1
        
        if out:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(results, indent=2, default=str))
            print_json({"status": "saved", "path": str(out), "summary": results["summary"]})
        else:
            print_json(results)
        
        client.close()

    asyncio.run(_run())


@db_app.command("export")
def db_export(
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    sample: int = typer.Option(10000, "--sample", help="Sample size per collection"),
    out_dir: Path = typer.Option(..., "--out-dir", help="Output directory for schema files"),
) -> None:
    """Export schemas for all collections to a directory."""
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        database = client[default_db]
        collection_names = await database.list_collection_names()
        
        out_dir.mkdir(parents=True, exist_ok=True)
        exported = []
        
        for coll_name in sorted(collection_names):
            if coll_name.startswith("system."):
                continue
            console.print(f"[dim]Exporting {coll_name}...[/dim]")
            result = await analyze_collection(client, default_db, coll_name, sample)
            out_path = out_dir / f"{coll_name}.yml"
            write_schema(out_path, result)
            exported.append(str(out_path))
        
        print_json({"status": "exported", "count": len(exported), "files": exported})
        client.close()

    asyncio.run(_run())


@db_app.command("drift")
def db_drift(
    schema_dir: Path = typer.Option(..., "--schema-dir", help="Directory with expected schema files"),
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    sample: int = typer.Option(5000, "--sample", help="Sample size per collection"),
) -> None:
    """Detect drift across all collections in the database."""
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        
        results = {
            "database": default_db,
            "collections": {},
            "summary": {"total": 0, "with_drift": 0, "critical": 0}
        }
        
        schema_files = list(schema_dir.glob("*.yml")) + list(schema_dir.glob("*.yaml"))
        
        for schema_path in sorted(schema_files):
            coll_name = schema_path.stem
            console.print(f"[dim]Checking drift for {coll_name}...[/dim]")
            
            expected_schema = load_schema(schema_path)
            observed = await analyze_collection(client, default_db, coll_name, sample)
            drift_result = detect_drift(expected_schema, observed)
            
            results["collections"][coll_name] = drift_result
            results["summary"]["total"] += 1
            if drift_result.get("has_drift"):
                results["summary"]["with_drift"] += 1
            if drift_result.get("critical_count", 0) > 0:
                results["summary"]["critical"] += 1
        
        print_json(results)
        client.close()
        
        if results["summary"]["with_drift"] > 0:
            raise typer.Exit(code=1)

    asyncio.run(_run())


@db_app.command("validate")
def db_validate(
    schema_dir: Path = typer.Option(..., "--schema-dir", help="Directory with schema files"),
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
    sample: int = typer.Option(10000, "--sample", help="Sample size per collection"),
    max_errors: int = typer.Option(100, "--max-errors", help="Max errors to return"),
) -> None:
    """Validate data in all collections against schemas."""
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        
        results = {
            "database": default_db,
            "collections": {},
            "summary": {"total_collections": 0, "valid_collections": 0, "invalid_collections": 0, "total_invalid_docs": 0}
        }
        
        schema_files = list(schema_dir.glob("*.yml")) + list(schema_dir.glob("*.yaml"))
        
        for schema_path in sorted(schema_files):
            coll_name = schema_path.stem
            console.print(f"[dim]Validating {coll_name}...[/dim]")
            
            validation_result = await validate_collection(
                client, 
                default_db, 
                coll_name, 
                load_schema(schema_path), 
                sample, 
                max_errors
            )
            
            results["collections"][coll_name] = validation_result
            results["summary"]["total_collections"] += 1
            
            if validation_result.get("invalid", 0) > 0:
                results["summary"]["invalid_collections"] += 1
                results["summary"]["total_invalid_docs"] += validation_result.get("invalid", 0)
            else:
                results["summary"]["valid_collections"] += 1
        
        print_json(results)
        client.close()
        
        if results["summary"]["total_invalid_docs"] > 0:
            raise typer.Exit(code=1)

    asyncio.run(_run())


@db_app.command("migrate")
def db_migrate(
    from_dir: Path = typer.Option(..., "--from-dir", help="Directory with source schema files"),

    to_dir: Path = typer.Option(..., "--to-dir", help="Directory with target schema files"),
    out_dir: Path = typer.Option(..., "--out-dir", help="Output directory for migration files"),
) -> None:
    """Generate migrations for all changed collections."""
    from_schemas = {p.stem: p for p in list(from_dir.glob("*.yml")) + list(from_dir.glob("*.yaml"))}
    to_schemas = {p.stem: p for p in list(to_dir.glob("*.yml")) + list(to_dir.glob("*.yaml"))}
    
    common = set(from_schemas.keys()) & set(to_schemas.keys())
    out_dir.mkdir(parents=True, exist_ok=True)
    
    migrations = []
    skipped = []
    
    for coll_name in sorted(common):
        from_schema = load_schema(from_schemas[coll_name])
        to_schema = load_schema(to_schemas[coll_name])
        
        diff = diff_schemas(from_schema, to_schema)
        summary = diff.get("summary", {})
        
        if summary.get("added", 0) == 0 and summary.get("removed", 0) == 0 and summary.get("changed", 0) == 0:
            skipped.append(coll_name)
            continue
        
        console.print(f"[dim]Generating migration for {coll_name}...[/dim]")
        out_path = out_dir / f"{coll_name}_migration.py"
        generate_migration_file(from_schema, to_schema, coll_name, out_path)
        migrations.append({"collection": coll_name, "path": str(out_path), "summary": summary})
    
    print_json({
        "status": "generated",
        "migrations": migrations,
        "skipped": skipped,
        "counts": {"generated": len(migrations), "skipped": len(skipped)}
    })


# =============================================================================
# Code Generation Commands
# =============================================================================


@generate_app.command("models")
def generate_models(
    schema: Path = typer.Option(..., "--schema", help="Schema file"),
    type: str = typer.Option(..., "--type", help="Output type: pydantic, typescript"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output file path"),
    name: str = typer.Option("Model", "--name", help="Class/Interface name"),
) -> None:
    """Generate Pydantic models or TypeScript interfaces from schema."""
    schema_data = load_schema(schema)
    
    if type.lower() == "pydantic":
        code = generate_pydantic_code(schema_data, name)
    elif type.lower() == "typescript":
        code = generate_typescript_code(schema_data, name)
    else:
        console.print(f"[red]Unknown type: {type}. Supported: pydantic, typescript[/red]")
        raise typer.Exit(code=1)
    
    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(code)
        print_json({"status": "generated", "path": str(out), "type": type})
    else:
        console.print(code)

# =============================================================================
# Documentation Commands
# =============================================================================


@docs_app.command("build")
def docs_build(
    schema_dir: Path = typer.Option(..., "--schema-dir", help="Directory with schema files"),
    out: Path = typer.Option(..., "--out", help="Output HTML file path"),
) -> None:
    """Generate static HTML documentation."""
    console.print(f"[dim]Generating documentation from {schema_dir}...[/dim]")
    generate_docs(schema_dir, out)
    print_json({"status": "generated", "path": str(out)})

# =============================================================================
# Seeding Commands
# =============================================================================


@app.command("seed")
def seed(
    schema: Path = typer.Option(..., "--schema", help="Schema file"),
    collection: str = typer.Option(..., "--collection", help="Collection name"),
    count: int = typer.Option(10, "--count", help="Number of documents to generate"),
    uri: Optional[str] = typer.Option(None, "--uri", help="MongoDB URI"),
    db: Optional[str] = typer.Option(None, "--db", help="Database name"),
) -> None:
    """Seed a collection with fake data based on schema."""
    async def _run() -> None:
        if uri and db:
            mongodb_uri = uri
            default_db = db
        else:
            config = load_runtime_config()
            mongodb_uri = uri or config.mongodb_uri
            default_db = db or config.default_db

        client = get_motor_client(mongodb_uri)
        schema_data = load_schema(schema)
        
        console.print(f"[dim]Seeding {count} documents into {collection}...[/dim]")
        inserted = await seed_collection(client, default_db, collection, schema_data, count)
        
        print_json({"status": "seeded", "inserted": inserted, "collection": collection})
        client.close()

    asyncio.run(_run())

# =============================================================================
# Git Hook Commands
# =============================================================================


@hook_app.command("install")
def hook_install(
    path: Path = typer.Option(Path(".pre-commit-config.yaml"), "--path", help="Path to config file"),
) -> None:
    """Install MongoSchematic pre-commit hooks."""
    install_hooks(path)
    console.print(f"[green]Successfully installed MongoSchematic hooks to {path}[/green]")

"""Rich-based reporting utilities for MongoSchematic CLI."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


def print_json(payload: Dict[str, Any]) -> None:
    """Print a JSON payload with syntax highlighting."""
    console.print(JSON(json.dumps(payload, indent=2)))


def print_schema_table(schema: Dict[str, Any]) -> None:
    """Print schema properties as a Rich table."""
    properties = schema.get("properties", {})
    required = set(schema.get("required", []))

    if not properties:
        console.print("[dim]No schema properties found.[/dim]")
        return

    table = Table(title="Schema Properties", show_header=True, header_style="bold cyan")
    table.add_column("Field", style="bold")
    table.add_column("Type", style="magenta")
    table.add_column("Presence", justify="right")
    table.add_column("Nullable")
    table.add_column("Required")

    for field, definition in sorted(properties.items()):
        if not isinstance(definition, dict):
            continue

        bson_type = definition.get("bsonType", "unknown")
        presence = definition.get("presence", 0)
        nullable = "Yes" if definition.get("nullable") else "No"
        is_required = "✓" if field in required else ""

        presence_str = f"{presence:.1%}" if isinstance(presence, (int, float)) else str(presence)

        table.add_row(field, bson_type, presence_str, nullable, is_required)

    console.print(table)


def print_diff_summary(diff: Dict[str, Any]) -> None:
    """Print a formatted diff summary with highlights."""
    summary = diff.get("summary", {})
    added = diff.get("added_fields", [])
    removed = diff.get("removed_fields", [])
    changed = diff.get("changed_fields", [])

    if summary.get("added", 0) == 0 and summary.get("removed", 0) == 0 and summary.get("changed", 0) == 0:
        console.print(Panel("[green]✓ No schema differences detected[/green]", title="Diff Summary"))
        return

    lines: List[str] = []

    if added:
        lines.append(f"[green]+{len(added)} added:[/green] {', '.join(added)}")

    if removed:
        lines.append(f"[red]-{len(removed)} removed:[/red] {', '.join(removed)}")

    if changed:
        changed_fields = [c.get("field", "?") for c in changed]
        lines.append(f"[yellow]~{len(changed)} changed:[/yellow] {', '.join(changed_fields)}")

    panel_content = "\n".join(lines)
    console.print(Panel(panel_content, title="Schema Diff", border_style="blue"))


def print_drift_report(drift: Dict[str, Any]) -> None:
    """Print a formatted drift detection report."""
    has_drift = drift.get("has_drift", False)
    drift_score = drift.get("drift_score", 0)
    critical = drift.get("critical_count", 0)
    warnings = drift.get("warning_count", 0)
    info = drift.get("info_count", 0)

    if not has_drift:
        console.print(Panel("[green]✓ No drift detected[/green]", title="Drift Report"))
        return

    score_color = "red" if drift_score > 0.5 else "yellow" if drift_score > 0.2 else "green"
    header = f"[{score_color}]Drift Score: {drift_score}[/{score_color}]"

    stats = f"[red]Critical: {critical}[/red] | [yellow]Warning: {warnings}[/yellow] | [dim]Info: {info}[/dim]"

    severity_items = drift.get("severity", [])
    details: List[str] = []
    for item in severity_items[:10]:
        level = item.get("level", "info")
        message = item.get("message", "Unknown")

        if level == "critical":
            details.append(f"[red]● {message}[/red]")
        elif level == "warning":
            details.append(f"[yellow]● {message}[/yellow]")
        else:
            details.append(f"[dim]● {message}[/dim]")

    if len(severity_items) > 10:
        details.append(f"[dim]... and {len(severity_items) - 10} more[/dim]")

    panel_content = f"{header}\n{stats}\n\n" + "\n".join(details)
    console.print(Panel(panel_content, title="Drift Report", border_style="red" if critical > 0 else "yellow"))


def print_anomalies(anomalies: List[Dict[str, Any]]) -> None:
    """Print detected anomalies with severity indicators."""
    if not anomalies:
        console.print("[dim]No anomalies detected.[/dim]")
        return

    table = Table(title="Anomalies Detected", show_header=True, header_style="bold yellow")
    table.add_column("Type", style="bold")
    table.add_column("Field")
    table.add_column("Details")

    for anomaly in anomalies[:20]:
        anomaly_type = anomaly.get("type", "UNKNOWN")
        field = anomaly.get("field", "?")
        details = anomaly.get("details", {})

        if anomaly_type == "MULTIPLE_TYPES":
            details_str = ", ".join(f"{k}: {v}" for k, v in details.items())
            type_style = "[red]MULTIPLE_TYPES[/red]"
        elif anomaly_type == "LOW_PRESENCE":
            details_str = f"presence: {details.get('presence', 0):.2%}"
            type_style = "[yellow]LOW_PRESENCE[/yellow]"
        elif anomaly_type == "HIGH_NULL_RATE":
            details_str = f"null_rate: {details.get('null_rate', 0):.2%}"
            type_style = "[yellow]HIGH_NULL_RATE[/yellow]"
        else:
            details_str = str(details)
            type_style = anomaly_type

        table.add_row(type_style, field, details_str)

    console.print(table)

    if len(anomalies) > 20:
        console.print(f"[dim]... and {len(anomalies) - 20} more anomalies[/dim]")


def print_recommendations(recommendations: List[Dict[str, Any]]) -> None:
    """Print recommendations with priority badges."""
    if not recommendations:
        console.print("[dim]No recommendations.[/dim]")
        return

    console.print()
    console.print("[bold]Recommendations[/bold]")
    console.print()

    for i, rec in enumerate(recommendations[:10], 1):
        rec_type = rec.get("type", "INFO")
        title = rec.get("title", "Untitled")
        description = rec.get("description", "")
        priority = rec.get("priority", "medium")

        if priority == "high" or rec_type == "DATA_QUALITY":
            badge = "[red]HIGH[/red]"
        elif priority == "low":
            badge = "[dim]LOW[/dim]"
        else:
            badge = "[yellow]MED[/yellow]"

        header = Text()
        header.append(f"{i}. ", style="dim")
        header.append(title, style="bold")

        console.print(f"  {badge} {header}")
        if description:
            console.print(f"     [dim]{description}[/dim]")

    if len(recommendations) > 10:
        console.print(f"\n[dim]... and {len(recommendations) - 10} more[/dim]")


def print_validation_summary(result: Dict[str, Any]) -> None:
    """Print a validation test summary."""
    valid = result.get("valid", 0)
    invalid = result.get("invalid", 0)
    total = valid + invalid
    errors = result.get("errors", [])

    if invalid == 0:
        console.print(Panel(
            f"[green]✓ All {total} sampled documents are valid[/green]",
            title="Validation Result"
        ))
        return

    pct_valid = (valid / total * 100) if total > 0 else 0
    summary = f"Valid: [green]{valid}[/green] ({pct_valid:.1f}%) | Invalid: [red]{invalid}[/red]"

    error_lines = []
    for err in errors[:5]:
        doc_id = err.get("_id", "?")
        issues = err.get("issues", [])
        issues_str = "; ".join(issues[:3])
        if len(issues) > 3:
            issues_str += f" (+{len(issues) - 3} more)"
        error_lines.append(f"[dim]{doc_id}:[/dim] {issues_str}")

    if len(errors) > 5:
        error_lines.append(f"[dim]... and {len(errors) - 5} more documents with errors[/dim]")

    panel_content = summary + "\n\n" + "\n".join(error_lines)
    console.print(Panel(panel_content, title="Validation Result", border_style="red"))

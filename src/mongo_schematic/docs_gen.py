from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from mongo_schematic import __version__
from mongo_schematic.schema_io import load_schema

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MongoSchematic Documentation</title>
    <style>
        :root {{
            --primary-color: #2563eb;
            --bg-color: #f8fafc;
            --sidebar-width: 280px;
            --border-color: #e2e8f0;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            margin: 0;
            display: flex;
            min-height: 100vh;
            background: var(--bg-color);
            color: #1e293b;
        }}
        .sidebar {{
            width: var(--sidebar-width);
            background: white;
            border-right: 1px solid var(--border-color);
            padding: 2rem 1.5rem;
            position: fixed;
            height: 100vh;
            overflow-y: auto;
        }}
        .main-content {{
            margin-left: var(--sidebar-width);
            padding: 3rem;
            flex: 1;
            max-width: 1000px;
        }}
        h1, h2, h3 {{ color: #0f172a; }}
        a {{ color: var(--primary-color); text-decoration: none; }}
        .nav-link {{
            display: block;
            padding: 0.75rem 1rem;
            border-radius: 0.5rem;
            margin-bottom: 0.25rem;
            color: #64748b;
        }}
        .nav-link:hover, .nav-link.active {{
            background: #eff6ff;
            color: var(--primary-color);
        }}
        .schema-card {{
            background: white;
            border-radius: 0.75rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            padding: 2rem;
            margin-bottom: 3rem;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 1.5rem;
        }}
        th {{
            text-align: left;
            padding: 0.75rem;
            background: #f8fafc;
            border-bottom: 2px solid var(--border-color);
            color: #64748b;
            font-weight: 600;
        }}
        td {{
            padding: 0.75rem;
            border-bottom: 1px solid var(--border-color);
        }}
        .badge {{
            display: inline-block;
            padding: 0.25rem 0.5rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
        }}
        .badge-req {{ background: #fee2e2; color: #991b1b; }}
        .badge-opt {{ background: #eff6ff; color: #1e40af; }}
        .badge-type {{ background: #f1f5f9; color: #475569; font-family: monospace; }}
        .meta {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
            padding-bottom: 2rem;
            border-bottom: 1px solid var(--border-color);
        }}
        .meta-item label {{ display: block; color: #64748b; font-size: 0.875rem; }}
        .meta-item span {{ font-weight: 600; }}
        .anomaly {{
            margin-top: 1rem;
            padding: 1rem;
            background: #fffbeb;
            border: 1px solid #fcd34d;
            border-radius: 0.5rem;
            color: #92400e;
        }}
    </style>
</head>
<body>
    <nav class="sidebar">
        <h3>MongoSchematic v{version}</h3>
        <div style="margin-top: 2rem;">
            {nav_items}
        </div>
    </nav>
    <main class="main-content">
        <h1>Database Documentation</h1>
        <p>Generated on {generated_at}</p>
        
        {content}
    </main>
</body>
</html>
"""

def generate_docs(schema_dir: Path, out_file: Path) -> None:
    """Generate static HTML documentation from schema directory."""
    files = sorted(list(schema_dir.glob("*.yml")) + list(schema_dir.glob("*.yaml")))
    
    nav_items = []
    content_blocks = []
    
    for schema_path in files:
        schema = load_schema(schema_path)
        coll_name = schema_path.stem.split(".")[0]
        
        nav_items.append(f'<a href="#{coll_name}" class="nav-link">{coll_name}</a>')
        
        props_html = []
        properties = schema.get("schema", {}).get("properties", {})
        required = set(schema.get("schema", {}).get("required", []))
        
        for field, details in properties.items():
            bson_type = details.get("bsonType", "any")
            req_badge = '<span class="badge badge-req">Required</span>' if field in required else '<span class="badge badge-opt">Optional</span>'
            presence = f"{details.get('presence', 0) * 100:.1f}%"
            
            props_html.append(f"""
            <tr>
                <td><strong>{field}</strong></td>
                <td><span class="badge badge-type">{bson_type}</span></td>
                <td>{req_badge}</td>
                <td>{presence}</td>
            </tr>
            """)
        
        anomalies_html = ""
        anomalies = schema.get("anomalies", [])
        if anomalies:
            items = "".join([f"<li>{a.get('type')}: {a.get('field')}</li>" for a in anomalies])
            anomalies_html = f"""
            <div class="anomaly">
                <strong>⚠️ Anomalies Detected:</strong>
                <ul>{items}</ul>
            </div>
            """

        content_blocks.append(f"""
        <div id="{coll_name}" class="schema-card">
            <h2>{coll_name}</h2>
            <div class="meta">
                <div class="meta-item">
                    <label>Collection</label>
                    <span>{schema.get('collection', coll_name)}</span>
                </div>
                <div class="meta-item">
                    <label>Database</label>
                    <span>{schema.get('database', 'N/A')}</span>
                </div>
                <div class="meta-item">
                    <label>Total Documents</label>
                    <span>{schema.get('total_documents', 'N/A')}</span>
                </div>
            </div>
            
            {anomalies_html}
            
            <table>
                <thead>
                    <tr>
                        <th>Field</th>
                        <th>Type</th>
                        <th>Required</th>
                        <th>Presence</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(props_html)}
                </tbody>
            </table>
        </div>
        """)
    
    html = HTML_TEMPLATE.format(
        version=__version__,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        nav_items="\n".join(nav_items),
        content="\n".join(content_blocks)
    )
    
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(html)

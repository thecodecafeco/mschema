from pathlib import Path
import yaml

PRE_COMMIT_CONFIG = {
    "repos": [
        {
            "repo": "local",
            "hooks": [
                {
                    "id": "mschema-drift",
                    "name": "MongoSchematic Drift Check",
                    "entry": "mschema db drift --schema-dir schemas/ --sample 1000",
                    "language": "system",
                    "pass_filenames": False,
                    "always_run": True,
                },
                {
                    "id": "mschema-validate",
                    "name": "MongoSchematic Validation",
                    "entry": "mschema db validate --schema-dir schemas/ --sample 1000",
                    "language": "system",
                    "pass_filenames": False,
                    "verbose": True,
                }
            ]
        }
    ]
}

def install_hooks(path: Path = Path(".pre-commit-config.yaml")) -> None:
    """Install pre-commit hooks for MongoSchematic."""
    
    if path.exists():
        content = yaml.safe_load(path.read_text())
        # Simplification: just return if file exists to avoid overwriting complex configs
        # In a real tool, we would merge
        print(f"Config {path} already exists. Please manually add MongoSchematic hooks.")
        return

    path.write_text(yaml.dump(PRE_COMMIT_CONFIG, sort_keys=False))

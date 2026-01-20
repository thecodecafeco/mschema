# MongoSchematic CLI Usage Guide

Complete guide for MongoDB schema analysis, drift detection, and migrations.

## Table of Contents

- [Getting Started](#getting-started)
- [Single Collection Workflows](#single-collection-workflows)
- [Database-Wide Workflows](#database-wide-workflows)
- [CI/CD Integration](#cicd-integration)
- [Monitoring & Alerts](#monitoring--alerts)
- [Troubleshooting](#troubleshooting)

---

## Getting Started

### Installation

```bash
pip install mongo-schematic
```

### Configuration

Create `.mschema.yml` in your project root:

```yaml
mongodb_uri: "mongodb://localhost:27017"
default_db: "myapp"
gemini_api_key: ""  # Optional, for AI recommendations
```

Or use environment variables:
```bash
export SCHEMAGEN_MONGODB_URI="mongodb://localhost:27017"
export SCHEMAGEN_DEFAULT_DB="myapp"
export SCHEMAGEN_GEMINI_API_KEY="your-key"
```

### First Analysis

```bash
mschema init                              # Create config file
mschema analyze --collection users        # Analyze a collection
```

---

## Single Collection Workflows

### 1. Schema Analysis

Infer schema from a live collection:

```bash
# Basic analysis
mschema analyze --collection users --sample 10000

# Save to file
mschema analyze --collection users --save schemas/users.yml

# With AI recommendations
mschema analyze --collection users --ai

# Store in MongoDB for history
mschema analyze --collection users --store
```

**Output includes:**
- Schema with field types and presence percentages
- Anomalies (multiple types, low presence, high null rates)
- Recommendations for schema improvements

### 2. Schema Export

Export schema to YAML:

```bash
mschema schema export --collection users --out schemas/users.v1.yml
```

### 3. Schema Diff

Compare two schema versions:

```bash
mschema schema diff --from schemas/users.v1.yml --to schemas/users.v2.yml
```

**Output:**
```json
{
  "added_fields": ["email_verified"],
  "removed_fields": ["legacy_id"],
  "changed_fields": [{"field": "age", "from": {"bsonType": "string"}, "to": {"bsonType": "int"}}],
  "summary": {"added": 1, "removed": 1, "changed": 1}
}
```

### 4. Drift Detection

Compare expected schema to live data:

```bash
mschema drift detect --schema schemas/users.v1.yml --collection users --sample 5000
```

**Output includes severity levels:**
- `critical` - Type changes on existing fields
- `warning` - Missing expected fields
- `info` - New fields detected in live data

### 5. Validation

Test documents against a schema:

```bash
# Test without modifying
mschema validate test --schema schemas/users.v1.yml --collection users --sample 10000

# Apply MongoDB native validator
mschema validate apply --schema schemas/users.v1.yml --collection users --level moderate --action error
```

### 6. Migration

#### Option A: Generate Python Migration File

```bash
mschema migrate create --from schemas/users.v1.yml --to schemas/users.v2.yml \
  --collection users --out migrations/20260120_users.py
```

Run the generated migration:
```python
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from migrations.20260120_users import Migration

async def run():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    migration = Migration(client, "myapp")
    await migration.up()    # Apply
    # await migration.down()  # Rollback

asyncio.run(run())
```

#### Option B: CLI-Driven Migration

```bash
# Generate plan
mschema migrate plan --from schemas/users.v1.yml --to schemas/users.v2.yml \
  --out plans/users.json

# Dry run
mschema migrate apply --plan plans/users.json --to schemas/users.v2.yml \
  --collection users --dry-run

# Apply with rate limiting
mschema migrate apply --plan plans/users.json --to schemas/users.v2.yml \
  --collection users --rate-limit-ms 50

# Resume from a specific document
mschema migrate apply --plan plans/users.json --to schemas/users.v2.yml \
  --collection users --resume-from 65aab12f8b6a9b7dd3cda901
```

### 7. Index Recommendations

```bash
# Get index recommendations
mschema schema recommend-indexes --schema schemas/users.v1.yml --collection users

# Check index usage
mschema schema index-usage --collection users
```

---

## Development Workflow

### Adding a New Collection

There are two ways to introduce a new collection:

#### 1. Code-First (Common)
Write your application code and let MongoDB create the collection data.
1. **Develop**: Write code that writes to a new collection (e.g., `audit_logs`).
2. **Run**: Run your app locally to generate some sample data.
3. **Capture**: Generate the schema from the data:
   ```bash
   mschema analyze --collection audit_logs --save schemas/audit_logs.yml
   ```

#### 2. Schema-First (Design-Driven)
Define the schema first, then generate code.
1. **Design**: Create `schemas/audit_logs.yml` manually.
2. **Generate**: Create Pydantic models or TypeScript interfaces:
   ```bash
   mschema generate models --schema schemas/audit_logs.yml --type pydantic --out src/models.py
   ```
3. **Develop**: Write your application code using the generated models.

### Ensuring Schemas Stay in Sync

When developers make changes to the application code (e.g., adding a field), the MongoDB schema changes implicitly. To ensure your `schemas/` directory stays in sync, you have two options:

#### Option 1: Manual Workflow
1. Developer modifies application code.
2. Developer runs the app (populating local DB with new fields).
3. Developer runs `mschema analyze` to update the schema file.
4. Developer commits both code and schema.

#### Option 2: Pre-Commit Hook (Recommended)
Automate the check to prevent forgetting step 3.

```bash
# 1. Install the hook
mschema hook install
```

This ensures that every PR includes accurate schema definitions corresponding to the code changes.

## Database-Wide Workflows

Analyze and manage schemas across all collections in a database.

### Analyze Entire Database

```bash
mschema db analyze --sample 5000
```

### Export All Schemas

```bash
mschema db export --out-dir schemas/
```

Creates one YAML file per collection:
```
schemas/
├── users.yml
├── products.yml
├── orders.yml
└── ...
```

### Database-Wide Drift Detection

```bash
mschema db drift --schema-dir schemas/ --sample 5000
```

### Database-Wide Validation

Validate all collections against their schemas:

```bash
mschema db validate --schema-dir schemas/ --sample 5000
```

### Generate All Migrations

```bash
mschema db migrate --from-dir schemas/v1/ --to-dir schemas/v2/ --out-dir migrations/
```

---

## CI/CD Integration

### How It Works

In a typical CI/CD pipeline (e.g., GitHub Actions), validation happens by comparing your **local code** against a **remote database**.

1. **Checkout**: The CI runner checks out your PR branch, which contains your modified schema definitions (e.g., `schemas/users.yml`).
2. **Connect**: `mschema` connects to your remote database (Staging/Dev) using the `MSCHEMA_MONGODB_URI` environment variable.
3. **Compare**:
    - **Drift Detection**: Compares your *local schema file* against the *actual structure* of the remote database. If fields are missing in the DB that are required in your schema, it flags them.
    - **Validation**: Fetches a sample of live documents from the remote DB and validates them against your *local schema constraints*.

### GitHub Actions

```yaml
name: Schema Validation
on:
  pull_request:
    paths:
      - "schemas/**"

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install MongoSchematic
        run: pip install mongo-schematic
      
      - name: Validate schemas against staging
        env:
          MSCHEMA_MONGODB_URI: ${{ secrets.STAGING_MONGODB_URI }}
          MSCHEMA_DEFAULT_DB: myapp
        run: |
          mschema validate test \
            --schema schemas/users.v1.yml \
            --collection users \
            --sample 20000
      
      - name: Check for drift
        env:
          MSCHEMA_MONGODB_URI: ${{ secrets.STAGING_MONGODB_URI }}
          MSCHEMA_DEFAULT_DB: myapp
        run: |
          mschema drift detect \
            --schema schemas/users.v1.yml \
            --collection users \
            --sample 10000
```

### GitLab CI

```yaml
stages: [test]

schema_validation:
  stage: test
  image: python:3.11
  script:
    - pip install mongo-schematic
    - mschema validate test --schema schemas/users.v1.yml --collection users
  variables:
    MSCHEMA_MONGODB_URI: $STAGING_MONGODB_URI
    MSCHEMA_DEFAULT_DB: myapp
```

### Jenkins

```groovy
pipeline {
    agent any
    environment {
        MSCHEMA_MONGODB_URI = credentials('staging-mongodb-uri')
        MSCHEMA_DEFAULT_DB = 'myapp'
    }
    stages {
        stage('Schema Validation') {
            steps {
                sh 'pip install mongo-schematic'
                sh 'mschema validate test --schema schemas/users.v1.yml --collection users'
            }
        }
    }
}
```

---

## Monitoring & Alerts

### Continuous Drift Monitoring

Run in background with webhook alerts:

```bash
mschema drift monitor --schema schemas/users.v1.yml --collection users \
  --interval 300 --webhook https://hooks.slack.com/services/xxx
```

### Webhook Payload

```json
{
  "added_fields": [],
  "removed_fields": ["deprecated_field"],
  "changed_fields": [],
  "severity": [{"level": "warning", "field": "deprecated_field", "message": "..."}],
  "drift_score": 0.15,
  "has_drift": true
}
```

---

## Troubleshooting

### Common Errors

| Error | Solution |
|-------|----------|
| `Missing MongoDB URI` | Set in `.mschema.yml` or `SCHEMAGEN_MONGODB_URI` |
| `Missing default DB` | Set in `.mschema.yml` or `SCHEMAGEN_DEFAULT_DB` |
| `Connection refused` | Check MongoDB is running and URI is correct |

### Debug Mode

For verbose output, use Python logging:

```bash
PYTHONPATH=src python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
from mschema.cli import app
app()
" analyze --collection users
```

### Getting Help

```bash
mschema --help
mschema analyze --help
mschema migrate --help
```

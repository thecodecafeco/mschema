# MongoSchematic CLI

**MongoSchematic** is a modern, developer-first tool for MongoDB schema management. It helps you infer schemas from existing data, detect drift, generate code, and handle migrations with confidence.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![MongoDB](https://img.shields.io/badge/mongodb-4.4+-green.svg)

## Features

- **Schema Inference**: Automatically infer schemas from your MongoDB collections.
- **Drift Detection**: Detect when your data deviates from your schema/models.
- **Code Generation**: Generate Pydantic models (Python) and TypeScript interfaces.
- **Documentation**: Build static HTML documentation for your database.
- **Data Seeding**: Populate your database with realistic dummy data.
- **Migrations**: Generate and execute versioned migration scripts.
- **Validation**: Validate documents against your defined schemas.
- **AI-Powered**: Optional Gemini AI integration for schema recommendations.

## Installation

```bash
pip install mongo-schematic
```

## Quick Start

### 1. Initialize

Generate a default configuration file:

```bash
mschema init
```

### 2. Analyze a Collection

Infer the schema from an existing collection and save it:

```bash
mschema analyze --collection users --save schemas/users.v1.yml
```

### 3. Generate Code

Create a Pydantic model for your application:

```bash
mschema generate models --schema schemas/users.v1.yml --type pydantic --out models/user.py
```

### 4. Detect Drift

Check if your live data matches your schema:

```bash
mschema drift detect --schema schemas/users.v1.yml --collection users
```

## Documentation

For full usage details including database-wide commands, CI/CD integration, and migration workflows, see the [Usage Guide](docs/USAGE.md).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

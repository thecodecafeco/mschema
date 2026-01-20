#!/bin/bash
# MongoSchematic Usage Demo
# This script demonstrates the core workflows of MongoSchematic

set -e

echo "Starting MongoSchematic Demo..."

# 1. Initialize
echo "\nInitializing configuration..."
if [ ! -f .mschema.yml ]; then
    mschema init
fi

# 2. Analyze
echo "\nAnalyzing 'users' collection..."
# Using dry-run/mock behavior if DB not available, or assuming DB exists
# for demo purposes we just show the commands
echo "Command: mschema analyze --collection users --save examples/schemas/users.v1.yml"
# mschema analyze --collection users --save examples/schemas/users.v1.yml

# 3. Generate Code
echo "\nGenerating Pydantic models..."
mschema generate models --schema examples/schemas/users.v1.yml --type pydantic --out examples/models/user.py
echo "Generated examples/models/user.py"

# 4. Generate Docs
echo "\nBuilding documentation..."
mschema docs build --schema-dir examples/schemas --out examples/docs/index.html

# 5. Seed Data
echo "\nSeeding data (Simulation)..."
echo "Command: mschema seed --schema examples/schemas/users.v1.yml --collection users_test --count 5"

echo "\nDemo complete!"

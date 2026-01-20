# Basic Workflow Example

This example shows a complete CLI workflow against a live MongoDB database.

## Prerequisites

- MongoDB reachable from your machine
- SchemaGen installed in your environment

## 1) Configure connection

Create a local config file:

```
cp .schemagen.example.yml .schemagen.yml
```

Edit `.schemagen.yml` and set your MongoDB URI and database name.

## 2) Export current schema (v1)

```
schemagen schema export --db myapp --collection users --out schemas/users.v1.yml
```

## 3) Apply app change (write new field)

Update your application to begin writing the new field (e.g., `roles`).

## 4) Export new schema (v2)

```
schemagen schema export --db myapp --collection users --out schemas/users.v2.yml
```

## 5) Diff schemas

```
schemagen schema diff --from schemas/users.v1.yml --to schemas/users.v2.yml
```

## 6) Create migration plan

```
schemagen migrate plan --from schemas/users.v1.yml --to schemas/users.v2.yml --out plans/users_v2.json
```

## 7) Dry-run migration

```
schemagen migrate apply --plan plans/users_v2.json --to schemas/users.v2.yml \
  --collection users --dry-run
```

## 8) Apply migration (batched)

```
schemagen migrate apply --plan plans/users_v2.json --to schemas/users.v2.yml \
  --collection users --rate-limit-ms 50
```

## 9) Apply MongoDB validation

```
schemagen validate apply --db myapp --schema schemas/users.v2.yml \
  --collection users --level moderate --action error
```

## 10) Turn on drift monitoring

```
schemagen drift monitor --db myapp --schema schemas/users.v2.yml \
  --collection users --interval 300 --webhook https://your-webhook-url
```

## Files in this example

- `schemas/users.v1.yml` - sample v1 schema
- `schemas/users.v2.yml` - sample v2 schema
- `plans/users_v2.json` - migration plan
- `migrations/20260120_users_v2.py` - migration stub (generated)
- `.schemagen.example.yml` - example config

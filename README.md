# migrate

A Git-aware database migration tool for PostgreSQL, built in Go.

Migrations are chained by SHA-256 hashes and linked to Git commits, making it possible to detect divergence from your main branch and know exactly how many migrations to revert.

## Install

```bash
go build -o bin/migrate ./cmd/migrate
```

## Usage

```bash
# Set your database URL
export DATABASE_URL="postgres://user:pass@localhost:5432/mydb?sslmode=disable"

# Apply all pending migrations
./bin/migrate up

# Check current state (detects dirty/diverged state)
./bin/migrate status

# Revert the last 2 migrations
./bin/migrate down 2

# Create a new migration (stamped with current git commit)
./bin/migrate create add_products_table

# Verify hash chain integrity (detect tampered migration files)
./bin/migrate verify
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--db` | `DATABASE_URL` env | PostgreSQL connection URL |
| `--dir` | `migrations` | Path to migration SQL files |
| `--main-branch` | `main` | Branch to compare against for dirty state detection |

## How It Works

### Hash Chain

Each applied migration is recorded in a `migration_hash_chain` table:

```
version | git_commit | parent_hash | entry_hash | checksum | applied_at
```

- `checksum` = SHA-256 of the migration file content
- `entry_hash` = SHA-256(parent_hash + checksum + version)
- `parent_hash` = the previous entry's `entry_hash` (genesis for the first)

This creates a tamper-evident chain — if any migration file is modified after being applied, `migrate verify` will detect it.

### Dirty State Detection

`migrate status` compares the applied hash chain in the database against the expected chain computed from migration files on the `main` branch (read via go-git, no checkout needed).

If they diverge, it reports:
- Which version the divergence started at
- How many migrations need to be reverted
- Which versions from `main` should be applied instead

## Migration Files

Migration files follow goose conventions:

```sql
-- +goose Up
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS users;
```

Name them with a numeric prefix: `001_create_users.sql`, `002_add_orders.sql`, etc.

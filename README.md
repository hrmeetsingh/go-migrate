# go-migrate

A Git-aware database migration tool for PostgreSQL, built in Go.

Migrations are chained by SHA-256 hashes and linked to Git commits, making it possible to detect divergence from your main branch, classify conflicts as soft or hard, and auto-resequence independent migrations.

## Architecture

The CLI layer exposes five commands that all route through a central Engine. The engine coordinates between three internal modules — `hashchain` for cryptographic integrity, `gitstate` for reading git history, and `sqlparse` for SQL conflict analysis — along with PostgreSQL and local migration files.

![Architecture](images/01_architecture.png)

## Install

**Option 1:** Install directly using `go install`:

```bash
go install github.com/hrmeetsingh/go-migrate/cmd/migrate@latest
```

This places the `migrate` binary in your `$GOPATH/bin` (or `$HOME/go/bin` by default).

**Option 2:** Clone and build from source:

```bash
git clone https://github.com/hrmeetsingh/go-migrate.git
cd go-migrate
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

| Flag            | Env Var          | Default      | Description                                         |
| --------------- | ---------------- | ------------ | --------------------------------------------------- |
| `--db`          | `DATABASE_URL`   | (none)       | PostgreSQL connection URL                           |
| `--dir`         | `MIGRATIONS_DIR` | `migrations` | Path to migration SQL files                         |
| `--main-branch` | `MAIN_BRANCH`    | `main`       | Branch to compare against for dirty state detection |

Precedence: CLI flag > environment variable > default value.

## What Go-Migrate Offers

### Tamper-Evident Hash Chain

Traditional migration tools track which migrations have been applied, and some store individual file checksums. Go-migrate goes further by building a blockchain-style linked chain where each entry depends on every previous one.

Each migration's `entry_hash` is computed from its parent's hash, its own file checksum, and its version number. This means tampering with *any* earlier migration breaks the chain for everything after it — not just that one file. Even reordering or inserting migrations would be caught.

![Hash Chain Construction](images/04_hash_chain.png)

The `verify` command walks the stored chain and recomputes every hash from scratch. If any file was modified after being applied, the recomputed hash won't match the stored one.

![Tamper Detection Flow](images/06_migrate_verify.png)

```bash
$ ./bin/migrate verify
Hash chain BROKEN at version 3!
A migration file may have been modified after it was applied.
```

If no tampering is found:

```bash
$ ./bin/migrate verify
Hash chain is valid. No tampering detected.
```

### Git-Branch-Aware Divergence Detection

When two developers create migrations on separate feature branches, merging one branch first leaves the other developer's database out of sync. Traditional tools will tell you "N pending," but they can't tell you that your applied history has diverged from the trunk.

Go-migrate reads migration files directly from the `main` branch using go-git (no checkout needed) and compares them against the applied chain in the database. It also records which branch each migration was applied from, so when divergence is detected you know exactly where to go to fix it.

Consider a real example from this project: migrations v1 through v4 are shared on `main` (`create_users`, `create_orders`, `add_unique_constraint`, `create_payments`). Three branches then diverge, each creating their own version 5 with completely different SQL — `offers_feature` adds an offers table, `notifications_feature` adds a notifications table, and `main` later adds a cart table. Same version number, different content, different SHA-256 checksums.

![Branch Divergence — Migration Conflict](images/07_branch_divergence_migrations.png)

Here is the detailed sequence of events when this happens across branches:

![Branch Divergence Scenario](images/05_branch_divergence.png)

`migrate status` detects and reports the divergence:

![Dirty State Detection](images/03_migrate_status.png)

```bash
$ ./bin/migrate status
Branch:          cart_feature
HEAD:            b4c5d6e
DB version:      5
Applied:         5
Pending:         0

!! DIRTY STATE DETECTED !!
Diverged at:     version 5
Revert count:    1 migrations
Versions to revert: [5]
Applied from:    notifications_feature
Then apply from main: [5]
>> Switch to branch 'notifications_feature' and run 'migrate down 1' to revert, then switch back and re-run 'migrate up'
```

### Smart Conflict Classification and Auto-Resequencing

Not all version collisions are equal. When a branch creates a **new independent table** that doesn't exist in main, the conflict is purely about the version number — the migration itself is safe to apply after renumbering. But when a branch **modifies tables defined in main** (ALTER, DROP, DML), that's a real schema conflict requiring manual intervention.

Go-migrate's `migrate up` command now performs **version-aware reconciliation** instead of a strict positional hash comparison. When it detects a version collision, it:

1. Parses the SQL in the colliding migration to extract operations (CREATE TABLE, ALTER TABLE, etc.)
2. Loads the entity catalog from the main branch (all tables created by main's migrations)
3. Classifies the conflict as **soft** or **hard**

![Conflict Classification](images/10_conflict_classification.png)

**Soft conflicts** — the migration only creates new entities not defined in main, or creates additive structures like indexes. The tool suggests resequencing the file to the next available version and asks for confirmation:

![Soft Conflict — Auto-Resequencing](images/11_soft_conflict_resequencing.png)

```bash
$ ./bin/migrate up

Reconciliation found 1 version collision(s):

  Version 003: LOCAL CREATE TABLE "notifications" (003_create_notifications.sql)
               DB has different migration applied from branch "feature-payments"
               Conflict: SOFT (independent new entity)

Suggested resequencing:
  003_create_notifications.sql  ->  004_create_notifications.sql

Proceed with resequencing? [y/N]: y
  Renamed: 003_create_notifications.sql -> 004_create_notifications.sql

Resequenced 1 file(s).

Note: DB contains 1 migration(s) from other branches (adopted)
Applied migration 4 (hash: a1b2c3d4e5f6..)
```

**Hard conflicts** — the migration alters, drops, or performs DML on entities defined in main's migrations. These cannot be auto-resolved and require manual intervention:

![Hard Conflict — Manual Resolution](images/12_hard_conflict.png)

```bash
$ ./bin/migrate up

!! HARD CONFLICT DETECTED !!

  Version 003: LOCAL ALTER TABLE "users" (003_add_email_to_users.sql)
               DB has different migration applied from branch "feature-payments"
               Conflict: HARD (modifies entities defined in main)

Cannot auto-resolve. Manual steps:
  1. Switch to branch 'feature-payments' and run 'migrate down 1' to revert
  2. Switch back and re-run 'migrate up'
Error: cannot apply migrations: hard conflict at version 3
```

The full `migrate up` flow, including the reconciliation and resequencing steps:

![Migrate Up — Reconciliation Flow](images/02_migrate_up.png)

### Git-Commit-Stamped Audit Trail

Every applied migration records the git commit and branch it was applied from in the `migration_hash_chain` table:

```
version | git_commit                               | parent_hash | entry_hash | checksum  | applied_branch         | applied_at
--------+------------------------------------------+-------------+------------+-----------+------------------------+------------------------
      1 | a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2 | 000000...   | 8f3a21...  | c4e9b1... | main                   | 2026-03-17 10:30:00+00
      2 | a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2 | 8f3a21...   | 7d2e44...  | b3f8a2... | main                   | 2026-03-17 10:30:01+00
      3 | f7e8d9c0b1a2f7e8d9c0b1a2f7e8d9c0b1a2f7e8 | 7d2e44...   | 5c1b33...  | a2d7f0... | notifications_feature  | 2026-03-18 14:20:00+00
```

During a production incident, you can trace any schema change back to the exact commit and branch that introduced it — something traditional tools cannot do since they only record version numbers and timestamps.

New migrations are also stamped with the current git commit when created:

```bash
$ ./bin/migrate create add_products_table
Created: migrations/003_add_products_table.sql
```

```sql
-- +goose Up
-- Migration: add_products_table
-- Git commit: a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2
-- Created: 2026-03-17T10:45:00Z

-- +goose Down
```

## How It Works

### Hash Chain

Each applied migration is recorded in a `migration_hash_chain` table:

```
version | git_commit | parent_hash | entry_hash | checksum | applied_branch | applied_at
```

- `checksum` = SHA-256 of the migration file content
- `entry_hash` = SHA-256(parent_hash + checksum + version)
- `parent_hash` = the previous entry's `entry_hash` (genesis for the first)
- `applied_branch` = the git branch that was checked out when the migration was applied

This creates a tamper-evident chain — if any migration file is modified after being applied, `migrate verify` will detect it.

### Version-Aware Reconciliation

When `migrate up` runs, it performs a version-aware comparison between local files and the DB chain:

1. **Verified** — DB version exists in local files with matching checksum. These are already applied and consistent.
2. **Foreign** — DB version does not exist in local files. This was applied from another branch and is "adopted" into the chain.
3. **Pending** — Local file version does not exist in DB. These will be applied.
4. **Collision** — DB version exists in local files but with a different checksum. These are classified as soft or hard.

For collisions, the SQL in the local migration is parsed and compared against the entity catalog from the main branch to determine whether it can be safely resequenced or requires manual resolution.

### Dirty State Detection

`migrate status` compares the applied hash chain in the database against the expected chain computed from the `main` branch's migration files (read via go-git, no checkout needed).

If they diverge, it reports:

- Which version the divergence started at
- How many migrations need to be reverted
- Which branch those migrations were applied from
- Which versions from `main` should be applied instead

### Conflict Classification Rules

| SQL Operation | Entity in main? | Classification |
|---|---|---|
| `CREATE TABLE new_table` | No | Soft |
| `CREATE TABLE existing_table` | Yes | Hard |
| `ALTER TABLE any_table` | Yes | Hard |
| `DROP TABLE any_table` | Yes | Hard |
| `CREATE INDEX ... ON any_table` | Either | Soft |
| `CREATE SEQUENCE` | -- | Soft |
| `DROP INDEX`, `DROP SEQUENCE` | -- | Hard |
| `INSERT/UPDATE/DELETE` | Yes | Hard |

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

# go-migrate

A Git-aware database migration tool for PostgreSQL, built in Go.

Migrations are chained by SHA-256 hashes and linked to Git commits, making it possible to detect divergence from your main branch and know exactly how many migrations to revert.

## Architecture

The CLI layer exposes five commands that all route through a central Engine. The engine coordinates between two internal modules — `hashchain` for cryptographic integrity and `gitstate` for reading git history — along with PostgreSQL and local migration files.

![Architecture](images/01_architecture.png)

## Install

**Option 1:** Install directly using `go install`:

```bash
go install github.com/harmeetsingh/go-migrate/cmd/migrate@latest
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

The following diagram walks through a realistic scenario: two feature branches both create version 5, leading to a divergence that go-migrate detects and guides you through resolving.

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

`migrate up` also catches this before applying anything:

![Migrate Up Flow](images/02_migrate_up.png)

```bash
$ ./bin/migrate up

!! DIRTY STATE DETECTED !!
Diverged at:     version 5
Revert count:    1 migrations
Versions to revert: [5]
Applied from:    notifications_feature
>> Switch to branch 'notifications_feature' and run 'migrate down 1' to revert, then switch back and re-run 'migrate up'
Error: cannot apply migrations: hash chain has diverged at version 5
```

Instead of silently saying "No pending migrations," the tool blocks the apply and tells you exactly which branch to switch to and what command to run.

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

### Dirty State Detection

Both `migrate status` and `migrate up` compare the applied hash chain in the database against the expected chain computed from local migration files (and from the `main` branch via go-git, no checkout needed).

If they diverge, it reports:

- Which version the divergence started at
- How many migrations need to be reverted
- Which branch those migrations were applied from
- Which versions from `main` should be applied instead

`migrate up` will refuse to apply new migrations if a divergence is detected, and will direct you to the correct branch to revert from first.

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

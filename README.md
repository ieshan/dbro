# dbro

Dbro is a lightweight, dialect-aware database migration toolkit for Go built on top of [GORM](https://gorm.io). It supports SQLite, PostgreSQL, and MySQL with a simple SQL-file-based workflow, flexible parser directives, and persistent migration tracking.

## Features

- **Dialect-aware** — SQLite, PostgreSQL, and MySQL with dialect-specific table creation
- **Flexible SQL parser** — `migrate:up`, `migrate:down`, `transaction:*`, and `statement:begin/end` blocks
- **Persistent tracking** — Applied migrations stored in a `dbro_migrations` table per database
- **Directory orchestration** — Apply or revert entire directories of timestamp-ordered migrations
- **Idempotent helpers** — `Once` variants guard against duplicate execution in-memory
- **Transaction safety** — Automatic transaction wrapping with MySQL DDL-awareness (DDL auto-commits)

## Installation

```bash
go get github.com/ieshan/dbro
```

Requires Go 1.22+ and GORM.

## Quick Start

### 1. Initialize the connection manager

```go
package main

import (
    "context"
    "log"

    "github.com/ieshan/dbro"
    "gorm.io/driver/sqlite"
    "gorm.io/gorm"
)

func main() {
    m := dbro.NewConnectionManager()

    // Register how to open a connection for a dialect
    m.AddConnectionFunc(dbro.DbSqlite, func(dsn string) (*gorm.DB, error) {
        return gorm.Open(sqlite.Open(dsn), &gorm.Config{})
    })

    // Register a named connection
    m.SetDsn("default", dbro.DbSqlite, "file:app.db?_fk=1")
}
```

### 2. Write a migration file

Create `migrations/2026-05-19-22-00-create-users.sql`:

```sql
-- migrate:up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- migrate:down
DROP TABLE users;
```

### 3. Run the migration

```go
ctx := context.Background()
if err := m.MigrateUp(ctx, "default", "migrations/2026-05-19-22-00-create-users.sql"); err != nil {
    log.Fatal(err)
}
```

## Writing Migration Files

Migration files are plain SQL with comment-based directives.

### Directives

| Directive | Description |
|-----------|-------------|
| `-- migrate:up` | Marks the start of the forward migration block |
| `-- migrate:down` | Marks the start of the rollback block |
| `-- transaction:true` | Wraps the migration in a transaction |
| `-- transaction:false` | Explicitly disables transaction wrapping |
| `-- statement:begin` | Start a multi-line statement escape block |
| `-- statement:end` | End the multi-line statement block |

### Examples

#### Transaction-enabled migration

```sql
-- transaction:true
-- migrate:up
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    total DECIMAL(10,2)
);
INSERT INTO orders (total) VALUES (99.99);

-- migrate:down
DROP TABLE orders;
```

#### Multi-line statement (PostgreSQL function)

```sql
-- migrate:up
-- statement:begin
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- statement:end

-- migrate:down
DROP FUNCTION IF EXISTS update_updated_at();
```

> **Note:** `statement:begin/end` prevents semicolon splitting inside the block so multi-line statements execute as a single unit.

## API Reference

### Connection Management

```go
m := dbro.NewConnectionManager()

// Register a dialect opener
m.AddConnectionFunc(dbro.DbPostgres, func(dsn string) (*gorm.DB, error) {
    return gorm.Open(postgres.Open(dsn), &gorm.Config{})
})

// Configure a named connection
m.SetDsn("main", dbro.DbPostgres, "host=localhost user=dbro password=dbro dbname=app sslmode=disable")
```

### Single-File Migrations

```go
// Apply a single migration file
err := m.MigrateUp(ctx, "main", "migrations/2026-05-19-22-00-add-users.sql")

// Rollback a single migration file
err := m.MigrateDown(ctx, "main", "migrations/2026-05-19-22-00-add-users.sql")

// Apply once (no-op if already tracked)
err := m.MigrateUpOnce(ctx, "main", "migrations/2026-05-19-22-00-add-users.sql")

// Rollback once (no-op if not tracked)
err := m.MigrateDownOnce(ctx, "main", "migrations/2026-05-19-22-00-add-users.sql")
```

### Directory Migrations

```go
// Apply all pending migrations in a directory (sorted by filename timestamp)
err := m.MigrateDir(ctx, "main", "migrations", "")

// Apply only up to a target version
err := m.MigrateDir(ctx, "main", "migrations", "2026-05-19-22-05")

// Revert to a target version (or "0" to revert all)
err := m.MigrateDir(ctx, "main", "migrations", "2026-05-19-22-00")

// Idempotent directory migration
err := m.MigrateDirOnce(ctx, "main", "migrations", "")
```

### File Naming Convention

Migration files should use a timestamp prefix followed by a descriptive name:

```
YYYY-MM-DD-HH-MM-description.sql
```

Examples:
- `2026-05-19-22-00-create-users.sql`
- `2026-05-19-22-05-add-email-index.sql`

## Transaction Behavior

| Dialect | DDL in transaction | Behavior |
|---------|-------------------|----------|
| SQLite | Supported | Full transaction wrapping |
| PostgreSQL | Supported | Full transaction wrapping |
| MySQL | Auto-commits DDL | Transaction wrapping is **disabled** automatically to avoid partial commit issues |

You can still force `transaction:true` for DML-only migrations on MySQL.

## Concurrent Safety

dbro provides database-level locking to prevent concurrent migration runners from
racing on the same database. Locking is **enabled by default**.

### How It Works

| Dialect | Lock mechanism | Scope |
|---------|---------------|-------|
| PostgreSQL | `pg_try_advisory_lock` with retry loop | Session (dedicated connection) |
| MySQL | `GET_LOCK` / `RELEASE_LOCK` | Session (dedicated connection) |
| SQLite/LibSQL | Table-based lock (`dbro_migration_lock` table) | Database (table row) |

### Configuration

```go
m := dbro.NewConnectionManager()

// Set lock timeout (default: 30s)
m.SetLockTimeout(60 * time.Second)

// Disable locking (e.g., for testing or single-process environments)
m.SetLockingEnabled(false)
```

### Important Notes

- **PostgreSQL/MySQL**: A dedicated connection is pinned from the pool for the
  duration of the lock. Ensure `MaxOpenConns >= 2` when locking is enabled
  (default). If `MaxOpenConns == 1`, dbro returns an error to prevent a deadlock.
- **SQLite**: Uses a table-based lock (`dbro_migration_lock` table). The table is
  created automatically on first use. If a process is killed while holding the lock,
  the row must be manually deleted (known limitation of table-based locks).
- **Context cancellation**: Locks are always released, even if the context is
  cancelled. The unlock operation uses `context.WithoutCancel` internally.
- **Error handling**: Lock timeout errors can be inspected with `errors.Is(err, dbro.ErrLockTimeout)`.
  If both migration and unlock fail, errors are joined via `errors.Join`.
- **`Once` variants**: `MigrateUpOnce`, `MigrateDownOnce`, and `MigrateDirOnce`
  provide both in-process protection (via mutex) and cross-process protection
  (via database lock).

## Legacy Compatibility

The original `RunMigration` and `RunMigrationOnce` methods are preserved and continue to split SQL by semicolons:

```go
err := m.RunMigration(ctx, "some_file.sql", false)
err := m.RunMigrationOnce(ctx, "some_file.sql", false)
```

## Testing

### Unit Tests

```bash
cd /Users/u2/go/src/github.com/ieshan/dbro
go test ./...
```

### Integration Tests (Docker)

A Docker Compose environment is provided for PostgreSQL and MySQL integration tests.

```bash
# Start databases and run tests
make setup
make test

# Stop services
make down
```

The test suite reads DSNs from environment variables:

```bash
export DBRO_POSTGRES_DSN="host=localhost user=dbro password=dbro dbname=dbro sslmode=disable"
export DBRO_MYSQL_DSN="dbro:dbro@tcp(localhost:3306)/dbro?parseTime=true"
```

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make setup` | Start Docker Compose services |
| `make test` | Run all tests (unit + integration) |
| `make vet` | Run `go vet` on all modules |
| `make fmt` | Format all Go code |
| `make build` | Build all packages |
| `make down` | Stop Docker Compose services |

## Project Structure

```
.
├── manager.go                     # Connection management
├── migration.go                   # Parser, ParsedMigration, version helpers
├── migration_models.go            # MigrationRecord and store helpers
├── migration_runner.go            # Core runner methods (Up/Down/Dir)
├── drop.go / flush.go             # Table cleanup utilities
├── migration_test.go              # Parser and version unit tests
├── tests/
│   ├── go.mod                     # Separate test module
│   ├── migration_test.go          # SQLite runner/model tests
│   └── migration_integration_test.go  # Postgres/MySQL integration tests
├── Makefile
├── compose.yml
└── go.work
```

## License

MIT

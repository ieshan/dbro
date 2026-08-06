package dbro

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

// ErrLockTimeout is returned when a database migration lock cannot be acquired
// within the configured timeout. Callers can inspect it with errors.Is.
var ErrLockTimeout = errors.New("migration lock timeout")

// ErrLockNotHeld is returned when Unlock is called without a successful Lock.
var ErrLockNotHeld = errors.New("migration lock not held")

// migrationLock provides database-level mutual exclusion for migration runs.
// Implementations MUST ensure that Lock and Unlock use the same underlying
// database connection for session-scoped locks (PostgreSQL, MySQL).
// This interface is unexported — it is an internal implementation detail.
type migrationLock interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

// postgresLock implements migrationLock using PostgreSQL advisory locks.
// A dedicated *sql.Conn is pinned for the lock lifetime because advisory locks
// are session-scoped.
type postgresLock struct {
	sqlDB        *sql.DB
	conn         *sql.Conn
	lockID       int64
	pollInterval time.Duration
	timeout      time.Duration
}

func (l *postgresLock) Lock(ctx context.Context) error {
	conn, err := l.sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire postgres connection for lock: %w", err)
	}

	deadline := time.Now().Add(l.timeout)
	for {
		if ctx.Err() != nil {
			conn.Close()
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			conn.Close()
			return fmt.Errorf("pg_try_advisory_lock: %w", ErrLockTimeout)
		}
		var locked bool
		err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", l.lockID).Scan(&locked)
		if err != nil {
			conn.Close()
			return fmt.Errorf("pg_try_advisory_lock: %w", err)
		}
		if locked {
			l.conn = conn
			return nil
		}
		select {
		case <-ctx.Done():
			conn.Close()
			return ctx.Err()
		case <-time.After(l.pollInterval):
		}
	}
}

func (l *postgresLock) Unlock(ctx context.Context) error {
	if l.conn == nil {
		return ErrLockNotHeld
	}
	_, err := l.conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", l.lockID)
	closeErr := l.conn.Close()
	l.conn = nil
	if err != nil {
		err = fmt.Errorf("pg_advisory_unlock: %w", err)
	}
	return errors.Join(err, closeErr)
}

// mysqlLock implements migrationLock using MySQL GET_LOCK/RELEASE_LOCK.
// A dedicated *sql.Conn is pinned because named locks are session-scoped.
type mysqlLock struct {
	sqlDB      *sql.DB
	conn       *sql.Conn
	lockName   string
	timeoutSec int
}

func (l *mysqlLock) Lock(ctx context.Context) error {
	conn, err := l.sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire mysql connection for lock: %w", err)
	}

	var result sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", l.lockName, l.timeoutSec).Scan(&result)
	if err != nil {
		conn.Close()
		return fmt.Errorf("GET_LOCK: %w", err)
	}
	if !result.Valid || result.Int64 != 1 {
		conn.Close()
		return fmt.Errorf("GET_LOCK: %w", ErrLockTimeout)
	}
	l.conn = conn
	return nil
}

func (l *mysqlLock) Unlock(ctx context.Context) error {
	if l.conn == nil {
		return ErrLockNotHeld
	}
	_, err := l.conn.ExecContext(context.WithoutCancel(ctx), "SELECT RELEASE_LOCK(?)", l.lockName)
	closeErr := l.conn.Close()
	l.conn = nil
	if err != nil {
		err = fmt.Errorf("RELEASE_LOCK: %w", err)
	}
	return errors.Join(err, closeErr)
}

// sqliteLock implements migrationLock using a table-based sentinel row.
// No dedicated connection is needed because INSERT OR IGNORE is a single
// statement that releases the database write lock immediately.
type sqliteLock struct {
	db           *sql.DB
	timeout      time.Duration
	pollInterval time.Duration
	locked       bool
}

func (l *sqliteLock) Lock(ctx context.Context) error {
	if _, err := l.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS dbro_migration_lock (id INTEGER PRIMARY KEY)"); err != nil {
		return fmt.Errorf("create lock table: %w", err)
	}

	deadline := time.Now().Add(l.timeout)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sqlite lock: %w", ErrLockTimeout)
		}
		result, err := l.db.ExecContext(ctx, "INSERT OR IGNORE INTO dbro_migration_lock (id) VALUES (1)")
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(l.pollInterval):
				continue
			}
		}
		rows, _ := result.RowsAffected()
		if rows == 1 {
			l.locked = true
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(l.pollInterval):
		}
	}
}

func (l *sqliteLock) Unlock(ctx context.Context) error {
	if !l.locked {
		return ErrLockNotHeld
	}
	l.locked = false
	_, err := l.db.ExecContext(context.WithoutCancel(ctx), "DELETE FROM dbro_migration_lock WHERE id = 1")
	if err != nil {
		return fmt.Errorf("release sqlite lock: %w", err)
	}
	return nil
}

// newLock creates a dialect-appropriate migrationLock for the named connection.
// For session-scoped locks (Postgres, MySQL) it guards against MaxOpenConns(1)
// which would exhaust the pool when pinning a connection.
func (m *ConnectionManager) newLock(ctx context.Context, name string) (migrationLock, error) {
	driver, err := m.driverNameFor(name)
	if err != nil {
		return nil, err
	}
	db, err := m.GetConnection(name)
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get underlying *sql.DB: %w", err)
	}

	if driver != DbSqlite && driver != DbLibSQL {
		stats := sqlDB.Stats()
		if stats.MaxOpenConnections == 1 {
			return nil, fmt.Errorf(
				"cannot enable database locking with MaxOpenConns(1) for %s: "+
					"the pinned lock connection would exhaust the pool; "+
					"increase MaxOpenConns or disable locking with SetLockingEnabled(false)",
				driver,
			)
		}
	}

	m.mu.RLock()
	lockTimeout := m.lockTimeout
	m.mu.RUnlock()

	switch driver {
	case DbPostgres:
		lockID := int64(crc32.ChecksumIEEE([]byte("dbro:" + name)))
		return &postgresLock{
			sqlDB:        sqlDB,
			lockID:       lockID,
			pollInterval: 1 * time.Second,
			timeout:      lockTimeout,
		}, nil
	case DbMySQL:
		return &mysqlLock{
			sqlDB:      sqlDB,
			lockName:   fmt.Sprintf("dbro_migrations:%s", name),
			timeoutSec: int(lockTimeout.Seconds()),
		}, nil
	case DbSqlite, DbLibSQL:
		return &sqliteLock{
			db:           sqlDB,
			timeout:      lockTimeout,
			pollInterval: 1 * time.Second,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported driver for migration lock: %s", driver)
	}
}

package dbro_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ieshan/dbro"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func newPostgresManager(t *testing.T) *dbro.ConnectionManager {
	t.Helper()
	dsn := os.Getenv("DBRO_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DBRO_POSTGRES_DSN not set")
	}
	m := dbro.NewConnectionManager()
	m.AddConnectionFunc(dbro.DbPostgres, func(dsn string) (*gorm.DB, error) {
		return gorm.Open(postgres.Open(dsn), &gorm.Config{})
	})
	m.SetDsn("default", dbro.DbPostgres, dsn)
	return m
}

func newMySQLManager(t *testing.T) *dbro.ConnectionManager {
	t.Helper()
	dsn := os.Getenv("DBRO_MYSQL_DSN")
	if dsn == "" {
		t.Skip("DBRO_MYSQL_DSN not set")
	}
	m := dbro.NewConnectionManager()
	m.AddConnectionFunc(dbro.DbMySQL, func(dsn string) (*gorm.DB, error) {
		return gorm.Open(mysql.Open(dsn), &gorm.Config{})
	})
	m.SetDsn("default", dbro.DbMySQL, dsn)
	return m
}

func TestIntegration_Postgres_MigrateUpDown_RoundTrip(t *testing.T) {
	m := newPostgresManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	f := writeFileAndGetPath(t, dir, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);
-- migrate:down
DROP TABLE users;`)

	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("migrate up failed: %v", err)
	}
	if err := m.MigrateDown(ctx, "default", f); err != nil {
		t.Fatalf("migrate down failed: %v", err)
	}
}

func TestIntegration_Postgres_MigrateDir_All(t *testing.T) {
	m := newPostgresManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-b.sql"), `-- migrate:up
CREATE TABLE b (id INT PRIMARY KEY);`)
	if err := m.MigrateDir(ctx, "default", dir, ""); err != nil {
		t.Fatalf("migrate dir failed: %v", err)
	}
}

func TestIntegration_Postgres_MigrateDir_RevertToTarget(t *testing.T) {
	m := newPostgresManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);
-- migrate:down
DROP TABLE a;`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-b.sql"), `-- migrate:up
CREATE TABLE b (id INT PRIMARY KEY);
-- migrate:down
DROP TABLE b;`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-02-c.sql"), `-- migrate:up
CREATE TABLE c (id INT PRIMARY KEY);
-- migrate:down
DROP TABLE c;`)
	if err := m.MigrateDir(ctx, "default", dir, ""); err != nil {
		t.Fatalf("migrate dir up failed: %v", err)
	}
	if err := m.MigrateDir(ctx, "default", dir, "2026-05-19-22-00"); err != nil {
		t.Fatalf("migrate dir down failed: %v", err)
	}
}

func TestIntegration_Postgres_TransactionTrue_Commit(t *testing.T) {
	m := newPostgresManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-tx.sql", `-- migrate:up transaction:true
CREATE TABLE t1 (id INT PRIMARY KEY);
CREATE TABLE t2 (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIntegration_Postgres_TransactionTrue_Rollback(t *testing.T) {
	m := newPostgresManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-05-tx-fail.sql", `-- migrate:up transaction:true
CREATE TABLE t3 (id INT PRIMARY KEY);
CREATE TABEL t4 (id INT PRIMARY KEY);`)
	err := m.MigrateUp(ctx, "default", f)
	if err == nil {
		t.Fatal("expected error for invalid SQL in transaction")
	}
	db, _ := m.GetConnection("default")
	var name string
	db.Raw("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 't3'").Scan(&name)
	if name != "" {
		t.Fatal("expected t3 to be rolled back")
	}
}

func TestIntegration_Postgres_ConcurrentApply(t *testing.T) {
	m1 := newPostgresManager(t)
	m2 := newPostgresManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-concurrent.sql", `-- migrate:up
CREATE TABLE concurrent (id INT PRIMARY KEY);`)

	var wg sync.WaitGroup
	var err1, err2 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = m1.MigrateUp(ctx, "default", f)
	}()
	go func() {
		defer wg.Done()
		err2 = m2.MigrateUp(ctx, "default", f)
	}()
	wg.Wait()
	if err1 != nil && err2 != nil {
		t.Fatalf("both managers failed: %v, %v", err1, err2)
	}
}

func TestIntegration_MySQL_MigrateUpDown_RoundTrip(t *testing.T) {
	m := newMySQLManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	f := writeFileAndGetPath(t, dir, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);
-- migrate:down
DROP TABLE users;`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("migrate up failed: %v", err)
	}
	if err := m.MigrateDown(ctx, "default", f); err != nil {
		t.Fatalf("migrate down failed: %v", err)
	}
}

func TestIntegration_MySQL_MigrateDir_All(t *testing.T) {
	m := newMySQLManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-b.sql"), `-- migrate:up
CREATE TABLE b (id INT PRIMARY KEY);`)
	if err := m.MigrateDir(ctx, "default", dir, ""); err != nil {
		t.Fatalf("migrate dir failed: %v", err)
	}
}

func TestIntegration_MySQL_TransactionTrue_Ignored(t *testing.T) {
	m := newMySQLManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-tx.sql", `-- migrate:up transaction:true
CREATE TABLE t1 (id INT PRIMARY KEY);
CREATE TABLE t2 (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIntegration_MySQL_dbroMigrations_TimestampPK(t *testing.T) {
	m := newMySQLManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-empty.sql", `-- migrate:up
SELECT 1;`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	db, _ := m.GetConnection("default")
	var colType string
	db.Raw("SELECT data_type FROM information_schema.columns WHERE table_name = 'dbro_migrations' AND column_name = 'id'").Scan(&colType)
	if colType != "timestamp" {
		t.Fatalf("expected id column type timestamp, got %q", colType)
	}
}

func TestIntegration_Postgres_ConcurrentMigrateDir_Serializes(t *testing.T) {
	m1 := newPostgresManager(t)
	m2 := newPostgresManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-b.sql"), `-- migrate:up
CREATE TABLE b (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-02-c.sql"), `-- migrate:up
CREATE TABLE c (id INT PRIMARY KEY);`)

	var wg sync.WaitGroup
	var err1, err2 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = m1.MigrateDir(ctx, "default", dir, "")
	}()
	go func() {
		defer wg.Done()
		err2 = m2.MigrateDir(ctx, "default", dir, "")
	}()
	wg.Wait()
	if err1 != nil {
		t.Fatalf("first MigrateDir failed: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("second MigrateDir failed: %v", err2)
	}

	// Verify no duplicate migration records
	db, _ := m1.GetConnection("default")
	var count int
	db.Raw("SELECT count(*) FROM dbro_migrations").Scan(&count)
	if count != 3 {
		t.Fatalf("expected 3 migration records, got %d", count)
	}
}

func TestIntegration_Postgres_LockReleasedOnContextCancel(t *testing.T) {
	mA := newPostgresManager(t)
	mB := newPostgresManager(t)
	ctx := context.Background()

	fA := writeTempSQL(t, "2026-05-19-22-00-slow.sql", `-- migrate:up
CREATE TABLE slow_a (id INT PRIMARY KEY);
SELECT pg_sleep(2);`)

	fB := writeTempSQL(t, "2026-05-19-22-01-b.sql", `-- migrate:up
CREATE TABLE b (id INT PRIMARY KEY);`)

	ctxA, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_ = mA.MigrateUp(ctxA, "default", fA)

	// Lock should be released via context.WithoutCancel in the defer.
	// Manager B (separate instance, same DSN) must be able to acquire the lock.
	if err := mB.MigrateUp(ctx, "default", fB); err != nil {
		t.Fatalf("manager B should succeed after lock release, got: %v", err)
	}
}

func TestIntegration_MySQL_ConcurrentMigrateDir_Serializes(t *testing.T) {
	m1 := newMySQLManager(t)
	m2 := newMySQLManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-b.sql"), `-- migrate:up
CREATE TABLE b (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-02-c.sql"), `-- migrate:up
CREATE TABLE c (id INT PRIMARY KEY);`)

	var wg sync.WaitGroup
	var err1, err2 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = m1.MigrateDir(ctx, "default", dir, "")
	}()
	go func() {
		defer wg.Done()
		err2 = m2.MigrateDir(ctx, "default", dir, "")
	}()
	wg.Wait()
	if err1 != nil {
		t.Fatalf("first MigrateDir failed: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("second MigrateDir failed: %v", err2)
	}

	// Verify no duplicate migration records
	db, _ := m1.GetConnection("default")
	var count int
	db.Raw("SELECT count(*) FROM dbro_migrations").Scan(&count)
	if count != 3 {
		t.Fatalf("expected 3 migration records, got %d", count)
	}
}

func writeFileAndGetPath(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	writeFile(t, path, content)
	return path
}

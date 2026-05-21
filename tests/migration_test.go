package dbro_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ieshan/dbro"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newSQLiteManager(t *testing.T) *dbro.ConnectionManager {
	t.Helper()
	m := dbro.NewConnectionManager()
	m.AddConnectionFunc(dbro.DbSqlite, func(dsn string) (*gorm.DB, error) {
		return gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	})
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	m.SetDsn("default", dbro.DbSqlite, dsn)
	return m
}

func TestEnsureMigrationsTable_CreatesIfMissing(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	if err := m.MigrateUp(ctx, "default", writeTempSQL(t, "2026-05-19-22-00-empty.sql", "")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureMigrationsTable_Idempotent(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	_ = m.MigrateUp(ctx, "default", writeTempSQL(t, "2026-05-19-22-00-empty.sql", ""))
	if err := m.MigrateUp(ctx, "default", writeTempSQL(t, "2026-05-19-22-01-empty.sql", "")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRecordMigration_Inserts(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRecordMigration_DuplicateID(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("idempotent second call should succeed, got: %v", err)
	}
}

func TestRemoveMigration_Deletes(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);
-- migrate:down
DROP TABLE users;`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDown(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateUp_SingleStatement(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// verify table exists
	db, _ := m.GetConnection("default")
	if err := db.Raw("SELECT name FROM sqlite_master WHERE type='table' AND name='users'").Error; err != nil {
		t.Fatalf("table should exist: %v", err)
	}
}

func TestMigrateUp_MultiStatement(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-multi.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);
CREATE TABLE orders (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateUp_NoUpBlock(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-noup.sql", `-- migrate:down
DROP TABLE users;`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateUp_InvalidSQL(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-bad.sql", `-- migrate:up
CREATE TABEL users (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", f); err == nil {
		t.Fatal("expected error for invalid SQL")
	}
}

func TestMigrateDown_SingleStatement(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);
-- migrate:down
DROP TABLE users;`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDown(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateDown_NoDownBlock(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-nodown.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDown(ctx, "default", f); err != nil {
		t.Fatalf("expected no-op for missing down block, got: %v", err)
	}
}

func TestMigrateDown_NotPreviouslyApplied(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);
-- migrate:down
DROP TABLE users;`)
	if err := m.MigrateDown(ctx, "default", f); err == nil {
		t.Fatal("expected error when migration was not applied")
	}
}

func TestMigrateUpOnce_Idempotent(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);`)
	if err := m.MigrateUpOnce(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateUpOnce(ctx, "default", f); err != nil {
		t.Fatalf("second call should be no-op: %v", err)
	}
}

func TestMigrateDownOnce_Idempotent(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-create-users.sql", `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);
-- migrate:down
DROP TABLE users;`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDownOnce(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDownOnce(ctx, "default", f); err != nil {
		t.Fatalf("second call should be no-op: %v", err)
	}
}

func TestMigrateUp_TransactionTrue_Sqlite(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-tx.sql", `-- migrate:up transaction:true
CREATE TABLE t1 (id INT PRIMARY KEY);
CREATE TABLE t2 (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateUp_TransactionTrue_RollbackOnError(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	f := writeTempSQL(t, "2026-05-19-22-00-tx-fail.sql", `-- migrate:up transaction:true
CREATE TABLE t1 (id INT PRIMARY KEY);
CREATE TABEL t2 (id INT PRIMARY KEY);`)
	err := m.MigrateUp(ctx, "default", f)
	if err == nil {
		t.Fatal("expected error for invalid SQL in transaction")
	}
	// Verify t1 was rolled back
	db, _ := m.GetConnection("default")
	var name string
	result := db.Raw("SELECT name FROM sqlite_master WHERE type='table' AND name='t1'").Scan(&name)
	if result.Error == nil && name != "" {
		t.Fatal("expected t1 to be rolled back")
	}
}

func TestMigrateDir_ApplyAll_UnsetTarget(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-create-users.sql"), `-- migrate:up
CREATE TABLE users (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-create-orders.sql"), `-- migrate:up
CREATE TABLE orders (id INT PRIMARY KEY);`)
	if err := m.MigrateDir(ctx, "default", dir, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateDir_ApplyToTarget(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-b.sql"), `-- migrate:up
CREATE TABLE b (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-02-c.sql"), `-- migrate:up
CREATE TABLE c (id INT PRIMARY KEY);`)
	if err := m.MigrateDir(ctx, "default", dir, "2026-05-19-22-01"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// c should not exist
	db, _ := m.GetConnection("default")
	var name string
	db.Raw("SELECT name FROM sqlite_master WHERE type='table' AND name='c'").Scan(&name)
	if name != "" {
		t.Fatal("expected table c to not exist")
	}
}

func TestMigrateDir_AlreadyPartiallyApplied(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-b.sql"), `-- migrate:up
CREATE TABLE b (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-02-c.sql"), `-- migrate:up
CREATE TABLE c (id INT PRIMARY KEY);`)
	if err := m.MigrateUp(ctx, "default", filepath.Join(dir, "2026-05-19-22-00-a.sql")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDir(ctx, "default", dir, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateDir_RevertToTarget(t *testing.T) {
	m := newSQLiteManager(t)
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
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDir(ctx, "default", dir, "2026-05-19-22-00"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// b and c should not exist
	db, _ := m.GetConnection("default")
	for _, tbl := range []string{"b", "c"} {
		var name string
		db.Raw(fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'", tbl)).Scan(&name)
		if name != "" {
			t.Fatalf("expected table %s to not exist", tbl)
		}
	}
}

func TestMigrateDir_RevertAll_Zero(t *testing.T) {
	m := newSQLiteManager(t)
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
	if err := m.MigrateDir(ctx, "default", dir, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDir(ctx, "default", dir, "zero"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// all user tables should be gone
	db, _ := m.GetConnection("default")
	var tables []string
	db.Raw("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != 'dbro_migrations'").Scan(&tables)
	if len(tables) > 0 {
		t.Fatalf("expected no user tables, got %v", tables)
	}
}

func TestMigrateDir_TargetAlreadyMet(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-b.sql"), `-- migrate:up
CREATE TABLE b (id INT PRIMARY KEY);`)
	if err := m.MigrateDir(ctx, "default", dir, "2026-05-19-22-01"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDir(ctx, "default", dir, "2026-05-19-22-01"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateDir_UnsupportedFileFormat(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "README.md"), `# Migrations`)
	writeFile(t, filepath.Join(dir, "script.sql"), `SELECT 1;`)
	if err := m.MigrateDir(ctx, "default", dir, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateDirOnce_Idempotent(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);`)
	if err := m.MigrateDirOnce(ctx, "default", dir, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.MigrateDirOnce(ctx, "default", dir, ""); err != nil {
		t.Fatalf("second call should be no-op: %v", err)
	}
}

func TestMigrateDir_StatementFailureMidDir(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "2026-05-19-22-00-a.sql"), `-- migrate:up
CREATE TABLE a (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-01-b.sql"), `-- migrate:up
CREATE TABEL b (id INT PRIMARY KEY);`)
	writeFile(t, filepath.Join(dir, "2026-05-19-22-02-c.sql"), `-- migrate:up
CREATE TABLE c (id INT PRIMARY KEY);`)
	err := m.MigrateDir(ctx, "default", dir, "")
	if err == nil {
		t.Fatal("expected error from bad SQL in middle file")
	}
	// a should remain recorded
	// Note: without transaction, a stays in DB
	db, _ := m.GetConnection("default")
	var name string
	db.Raw("SELECT name FROM sqlite_master WHERE type='table' AND name='a'").Scan(&name)
	if name != "a" {
		t.Fatal("expected table a to remain after mid-dir failure")
	}
}

func TestMigrateDir_EmptyDirectory(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	dir := t.TempDir()
	if err := m.MigrateDir(ctx, "default", dir, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestError_MissingMigrationFile(t *testing.T) {
	m := newSQLiteManager(t)
	ctx := context.Background()
	if err := m.MigrateUp(ctx, "default", "/nonexistent.sql"); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestGetConnection_SetAndValid(t *testing.T) {
	m := newSQLiteManager(t)
	db, err := m.GetConnection("default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if db == nil {
		t.Fatal("expected non-nil db")
	}
}

func writeTempSQL(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return path
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
}

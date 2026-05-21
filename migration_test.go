package dbro

import (
	"testing"
	"time"
)

func TestParseMigration_UpOnly(t *testing.T) {
	sql := `-- migrate:up
CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 1 {
		t.Fatalf("expected 1 up statement, got %d", len(parsed.UpStatements))
	}
	if len(parsed.DownStatements) != 0 {
		t.Fatalf("expected 0 down statements, got %d", len(parsed.DownStatements))
	}
	if parsed.UseTransaction {
		t.Fatal("expected UseTransaction false")
	}
}

func TestParseMigration_DownOnly(t *testing.T) {
	sql := `-- migrate:down
DROP TABLE t;`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 0 {
		t.Fatalf("expected 0 up statements, got %d", len(parsed.UpStatements))
	}
	if len(parsed.DownStatements) != 1 {
		t.Fatalf("expected 1 down statement, got %d", len(parsed.DownStatements))
	}
}

func TestParseMigration_UpAndDown(t *testing.T) {
	sql := `-- migrate:up
CREATE TABLE t (id INT);
-- migrate:down
DROP TABLE t;`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 1 {
		t.Fatalf("expected 1 up statement, got %d", len(parsed.UpStatements))
	}
	if len(parsed.DownStatements) != 1 {
		t.Fatalf("expected 1 down statement, got %d", len(parsed.DownStatements))
	}
}

func TestParseMigration_NoAnnotations(t *testing.T) {
	sql := `CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 0 || len(parsed.DownStatements) != 0 {
		t.Fatalf("expected empty blocks, got up=%d down=%d", len(parsed.UpStatements), len(parsed.DownStatements))
	}
}

func TestParseMigration_TransactionBeforeMigrate(t *testing.T) {
	sql := `-- transaction:true
-- migrate:up
CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !parsed.UseTransaction {
		t.Fatal("expected UseTransaction true")
	}
}

func TestParseMigration_TransactionAfterMigrate(t *testing.T) {
	sql := `-- migrate:up transaction:true
CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !parsed.UseTransaction {
		t.Fatal("expected UseTransaction true")
	}
}

func TestParseMigration_TransactionFalse(t *testing.T) {
	sql := `-- migrate:up transaction:false
CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.UseTransaction {
		t.Fatal("expected UseTransaction false")
	}
}

func TestParseMigration_TransactionNoToken(t *testing.T) {
	sql := `-- migrate:up
CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.UseTransaction {
		t.Fatal("expected UseTransaction false")
	}
}

func TestParseMigration_MultipleDashes(t *testing.T) {
	sql := `--- migrate:up
CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 1 {
		t.Fatalf("expected 1 up statement, got %d", len(parsed.UpStatements))
	}
}

func TestParseMigration_VariedWhitespace(t *testing.T) {
	sql := `--   migrate:up    transaction:true  
CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 1 {
		t.Fatalf("expected 1 up statement, got %d", len(parsed.UpStatements))
	}
	if !parsed.UseTransaction {
		t.Fatal("expected UseTransaction true")
	}
}

func TestParseMigration_GenuineSQLCommentsIgnored(t *testing.T) {
	sql := `-- migrate:up
-- create index on users
CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 1 {
		t.Fatalf("expected 1 up statement, got %d", len(parsed.UpStatements))
	}
}

func TestParseMigration_MultiStatementBlock(t *testing.T) {
	sql := `-- migrate:up
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 2 {
		t.Fatalf("expected 2 up statements, got %d", len(parsed.UpStatements))
	}
}

func TestParseMigration_StatementBeginEnd(t *testing.T) {
	sql := `-- migrate:up
-- statement:begin
CREATE OR REPLACE FUNCTION foo() RETURNS void AS $$
BEGIN
  PERFORM 1;
END;
$$ LANGUAGE plpgsql;
-- statement:end
CREATE TABLE t (id INT);`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 2 {
		t.Fatalf("expected 2 up statements, got %d: %v", len(parsed.UpStatements), parsed.UpStatements)
	}
}

func TestParseMigration_EmptyFile(t *testing.T) {
	parsed, err := ParseMigration("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 0 || len(parsed.DownStatements) != 0 {
		t.Fatalf("expected empty blocks")
	}
}

func TestParseMigration_OnlyComments(t *testing.T) {
	sql := `-- just a comment
-- another comment`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 0 || len(parsed.DownStatements) != 0 {
		t.Fatalf("expected empty blocks")
	}
}

func TestParseMigration_TrailingSemicolons(t *testing.T) {
	sql := `-- migrate:up
CREATE TABLE t (id INT);
;
;`
	parsed, err := ParseMigration(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.UpStatements) != 1 {
		t.Fatalf("expected 1 up statement, got %d", len(parsed.UpStatements))
	}
}

func TestParseMigrationFileName_Valid(t *testing.T) {
	ver, desc, err := parseMigrationFileName("2026-05-19-22-00-create-users.sql")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2026, 5, 19, 22, 0, 0, 0, time.UTC)
	if !ver.Equal(expected) {
		t.Fatalf("expected version %v, got %v", expected, ver)
	}
	if desc != "create-users" {
		t.Fatalf("expected description 'create-users', got %q", desc)
	}
}

func TestParseMigrationFileName_NoExtension(t *testing.T) {
	_, _, err := parseMigrationFileName("2026-05-19-22-00-create-users")
	if err == nil {
		t.Fatal("expected error for missing .sql extension")
	}
}

func TestParseMigrationFileName_BadTimestamp(t *testing.T) {
	_, _, err := parseMigrationFileName("2026-13-19-22-00-create-users.sql")
	if err == nil {
		t.Fatal("expected error for invalid month")
	}
}

func TestParseMigrationFileName_EmptyDescription(t *testing.T) {
	ver, desc, err := parseMigrationFileName("2026-05-19-22-00-.sql")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2026, 5, 19, 22, 0, 0, 0, time.UTC)
	if !ver.Equal(expected) {
		t.Fatalf("expected version %v, got %v", expected, ver)
	}
	if desc != "" {
		t.Fatalf("expected empty description, got %q", desc)
	}
}

func TestParseMigrationFileName_NonTimestampPrefix(t *testing.T) {
	_, _, err := parseMigrationFileName("abc-create-users.sql")
	if err == nil {
		t.Fatal("expected error for non-timestamp prefix")
	}
}

func TestVersionFromString_Valid(t *testing.T) {
	tm, err := versionFromString("2026-05-19-22-00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2026, 5, 19, 22, 0, 0, 0, time.UTC)
	if !tm.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, tm)
	}
}

func TestVersionFromString_Malformed(t *testing.T) {
	_, err := versionFromString("2026-05-19")
	if err == nil {
		t.Fatal("expected error for malformed version string")
	}
}

func TestVersionString_RoundTrip(t *testing.T) {
	original := time.Date(2026, 5, 19, 22, 0, 0, 0, time.UTC)
	s := versionString(original)
	if s != "2026-05-19-22-00" {
		t.Fatalf("expected '2026-05-19-22-00', got %q", s)
	}
	parsed, err := versionFromString(s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !parsed.Equal(original) {
		t.Fatalf("round-trip failed: expected %v, got %v", original, parsed)
	}
}

func FuzzParseMigration(f *testing.F) {
	f.Add("-- migrate:up\nCREATE TABLE t (id INT);")
	f.Add("-- migrate:up transaction:true\nINSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2);")
	f.Add("-- statement:begin\nSELECT 1;\n-- statement:end\n")
	f.Add("")
	f.Add("-- just a comment")
	f.Fuzz(func(t *testing.T, sql string) {
		_, _ = ParseMigration(sql)
	})
}

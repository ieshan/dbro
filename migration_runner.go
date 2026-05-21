package dbro

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

// runStatements executes a slice of SQL statements against the provided db.
// If inTx is true, the statements run inside a GORM transaction.
func runStatements(ctx context.Context, db *gorm.DB, statements []string, filePath string, inTx bool) error {
	execFn := func(tx *gorm.DB) error {
		for i, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if err := tx.WithContext(ctx).Exec(stmt).Error; err != nil {
				return fmt.Errorf("statement %d in %s: %w\nStatement: %s", i+1, filePath, err, stmt)
			}
		}
		return nil
	}

	if inTx {
		return db.WithContext(ctx).Transaction(execFn)
	}
	return execFn(db.WithContext(ctx))
}

// MigrateUp applies the up block of a single migration file and records it.
func (m *ConnectionManager) MigrateUp(ctx context.Context, name, filePath string) error {
	if err := m.ensureMigrationsTable(ctx, name); err != nil {
		return fmt.Errorf("ensure migrations table: %w", err)
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read migration file %s: %w", filePath, err)
	}
	parsed, err := ParseMigration(string(content))
	if err != nil {
		return fmt.Errorf("parse migration %s: %w", filePath, err)
	}

	if len(parsed.UpStatements) == 0 {
		return nil
	}

	version, _, err := parseMigrationFileName(filepath.Base(filePath))
	if err != nil {
		return fmt.Errorf("extract version from %s: %w", filePath, err)
	}

	applied, err := m.getAppliedMigrations(ctx, name)
	if err != nil {
		return fmt.Errorf("check applied migrations: %w", err)
	}
	for _, rec := range applied {
		if rec.ID.Equal(version) {
			return nil
		}
	}

	db, err := m.GetConnection(name)
	if err != nil {
		return err
	}
	driver, err := m.driverNameFor(name)
	if err != nil {
		return err
	}
	useTx := parsed.UseTransaction && driver != DbMySQL

	if err := runStatements(ctx, db, parsed.UpStatements, filePath, useTx); err != nil {
		return fmt.Errorf("migrate up %s: %w", filePath, err)
	}

	if err := m.recordMigration(ctx, name, version); err != nil {
		return fmt.Errorf("record migration %s: %w", filePath, err)
	}
	return nil
}

// MigrateDown reverts the down block of a single migration file and removes its record.
func (m *ConnectionManager) MigrateDown(ctx context.Context, name, filePath string) error {
	if err := m.ensureMigrationsTable(ctx, name); err != nil {
		return fmt.Errorf("ensure migrations table: %w", err)
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read migration file %s: %w", filePath, err)
	}
	parsed, err := ParseMigration(string(content))
	if err != nil {
		return fmt.Errorf("parse migration %s: %w", filePath, err)
	}

	if len(parsed.DownStatements) == 0 {
		return nil
	}

	version, _, err := parseMigrationFileName(filepath.Base(filePath))
	if err != nil {
		return fmt.Errorf("extract version from %s: %w", filePath, err)
	}

	applied, err := m.getAppliedMigrations(ctx, name)
	if err != nil {
		return fmt.Errorf("check applied migrations: %w", err)
	}
	found := false
	for _, rec := range applied {
		if rec.ID.Equal(version) {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("migration %s was not applied, cannot revert", filePath)
	}

	db, err := m.GetConnection(name)
	if err != nil {
		return err
	}
	driver, err := m.driverNameFor(name)
	if err != nil {
		return err
	}
	useTx := parsed.UseTransaction && driver != DbMySQL

	if err := runStatements(ctx, db, parsed.DownStatements, filePath, useTx); err != nil {
		return fmt.Errorf("migrate down %s: %w", filePath, err)
	}

	if err := m.removeMigration(ctx, name, version); err != nil {
		return fmt.Errorf("remove migration %s: %w", filePath, err)
	}
	return nil
}

// MigrateUpOnce runs MigrateUp only once per file path in this process lifetime.
func (m *ConnectionManager) MigrateUpOnce(ctx context.Context, name, filePath string) error {
	key := "up:" + name + ":" + filePath
	m.migrationMu.RLock()
	_, exists := m.executedMigrations[key]
	m.migrationMu.RUnlock()
	if exists {
		return nil
	}

	m.migrationMu.Lock()
	defer m.migrationMu.Unlock()
	if _, exists = m.executedMigrations[key]; exists {
		return nil
	}
	if err := m.MigrateUp(ctx, name, filePath); err != nil {
		return err
	}
	m.executedMigrations[key] = struct{}{}
	return nil
}

// MigrateDownOnce runs MigrateDown only once per file path in this process lifetime.
func (m *ConnectionManager) MigrateDownOnce(ctx context.Context, name, filePath string) error {
	key := "down:" + name + ":" + filePath
	m.migrationMu.RLock()
	_, exists := m.executedMigrations[key]
	m.migrationMu.RUnlock()
	if exists {
		return nil
	}

	m.migrationMu.Lock()
	defer m.migrationMu.Unlock()
	if _, exists = m.executedMigrations[key]; exists {
		return nil
	}
	if err := m.MigrateDown(ctx, name, filePath); err != nil {
		return err
	}
	m.executedMigrations[key] = struct{}{}
	return nil
}

// MigrateDir orchestrates migrations in a directory based on a target version.
// targetVersion follows the rules documented in the migration plan.
func (m *ConnectionManager) MigrateDir(ctx context.Context, name, dirPath string, targetVersion string) error {
	if err := m.ensureMigrationsTable(ctx, name); err != nil {
		return fmt.Errorf("ensure migrations table: %w", err)
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("read migration directory %s: %w", dirPath, err)
	}

	type migrationFile struct {
		version time.Time
		path    string
	}
	var files []migrationFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		ver, _, err := parseMigrationFileName(entry.Name())
		if err != nil {
			continue
		}
		files = append(files, migrationFile{version: ver, path: filepath.Join(dirPath, entry.Name())})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].version.Before(files[j].version)
	})

	applied, err := m.getAppliedMigrations(ctx, name)
	if err != nil {
		return fmt.Errorf("get applied migrations: %w", err)
	}

	appliedSet := make(map[time.Time]bool)
	var maxApplied time.Time
	for _, rec := range applied {
		appliedSet[rec.ID] = true
		if rec.ID.After(maxApplied) {
			maxApplied = rec.ID
		}
	}

	switch {
	case targetVersion == "":
		for _, f := range files {
			if !appliedSet[f.version] {
				if err := m.MigrateUp(ctx, name, f.path); err != nil {
					return fmt.Errorf("apply %s: %w", f.path, err)
				}
			}
		}

	case targetVersion == "zero":
		for i := len(files) - 1; i >= 0; i-- {
			if appliedSet[files[i].version] {
				if err := m.MigrateDown(ctx, name, files[i].path); err != nil {
					return fmt.Errorf("revert %s: %w", files[i].path, err)
				}
			}
		}

	default:
		targetVer, err := versionFromString(targetVersion)
		if err != nil {
			return fmt.Errorf("parse target version %q: %w", targetVersion, err)
		}

		if targetVer.After(maxApplied) || targetVer.Equal(maxApplied) {
			for _, f := range files {
				if !appliedSet[f.version] && (f.version.Before(targetVer) || f.version.Equal(targetVer)) {
					if err := m.MigrateUp(ctx, name, f.path); err != nil {
						return fmt.Errorf("apply %s: %w", f.path, err)
					}
				}
			}
		} else {
			for i := len(files) - 1; i >= 0; i-- {
				if appliedSet[files[i].version] && files[i].version.After(targetVer) {
					if err := m.MigrateDown(ctx, name, files[i].path); err != nil {
						return fmt.Errorf("revert %s: %w", files[i].path, err)
					}
				}
			}
		}
	}
	return nil
}

// MigrateDirOnce runs MigrateDir only once per (name, dirPath, targetVersion) in this process lifetime.
func (m *ConnectionManager) MigrateDirOnce(ctx context.Context, name, dirPath string, targetVersion string) error {
	key := fmt.Sprintf("dir:%s:%s:%s", name, dirPath, targetVersion)
	m.migrationMu.RLock()
	_, exists := m.executedMigrations[key]
	m.migrationMu.RUnlock()
	if exists {
		return nil
	}

	m.migrationMu.Lock()
	defer m.migrationMu.Unlock()
	if _, exists = m.executedMigrations[key]; exists {
		return nil
	}
	if err := m.MigrateDir(ctx, name, dirPath, targetVersion); err != nil {
		return err
	}
	m.executedMigrations[key] = struct{}{}
	return nil
}

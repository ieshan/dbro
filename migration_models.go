package dbro

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// MigrationRecord represents an applied migration in the dbro_migrations table.
type MigrationRecord struct {
	ID        time.Time `gorm:"column:id;primaryKey"`
	AppliedAt time.Time `gorm:"column:applied_at"`
}

// TableName overrides GORM's default pluralization.
func (MigrationRecord) TableName() string {
	return "dbro_migrations"
}

// CreateMigrationsTable creates the dbro_migrations tracking table if it does not exist.
func CreateMigrationsTable(ctx context.Context, db *gorm.DB, driverName string) error {
	var ddl string
	switch driverName {
	case DbSqlite, DbLibSQL:
		ddl = `CREATE TABLE IF NOT EXISTS dbro_migrations (
			id DATETIME PRIMARY KEY,
			applied_at DATETIME
		)`
	case DbPostgres:
		ddl = `CREATE TABLE IF NOT EXISTS dbro_migrations (
			id TIMESTAMP PRIMARY KEY,
			applied_at TIMESTAMP NOT NULL
		)`
	case DbMySQL:
		ddl = `CREATE TABLE IF NOT EXISTS dbro_migrations (
			id TIMESTAMP PRIMARY KEY,
			applied_at TIMESTAMP NOT NULL
		)`
	default:
		return fmt.Errorf("unsupported driver for migrations table: %s", driverName)
	}
	return db.WithContext(ctx).Exec(ddl).Error
}

// ensureMigrationsTable creates the tracking table on the named connection if needed.
func (m *ConnectionManager) ensureMigrationsTable(ctx context.Context, name string) error {
	db, err := m.GetConnection(name)
	if err != nil {
		return err
	}
	driver, err := m.driverNameFor(name)
	if err != nil {
		return err
	}
	return CreateMigrationsTable(ctx, db, driver)
}

// getAppliedMigrations returns all applied migrations ordered by id ascending.
func (m *ConnectionManager) getAppliedMigrations(ctx context.Context, name string) ([]MigrationRecord, error) {
	db, err := m.GetConnection(name)
	if err != nil {
		return nil, err
	}

	var records []MigrationRecord
	if err := db.WithContext(ctx).Order("id ASC").Find(&records).Error; err != nil {
		return nil, fmt.Errorf("failed to read applied migrations: %w", err)
	}
	return records, nil
}

// recordMigration inserts a migration record into dbro_migrations.
func (m *ConnectionManager) recordMigration(ctx context.Context, name string, id time.Time) error {
	db, err := m.GetConnection(name)
	if err != nil {
		return err
	}

	rec := MigrationRecord{ID: id, AppliedAt: time.Now().UTC()}
	if err := db.WithContext(ctx).Create(&rec).Error; err != nil {
		return fmt.Errorf("failed to record migration %s: %w", versionString(id), err)
	}
	return nil
}

// removeMigration deletes a migration record from dbro_migrations by id.
func (m *ConnectionManager) removeMigration(ctx context.Context, name string, id time.Time) error {
	db, err := m.GetConnection(name)
	if err != nil {
		return err
	}

	if err := db.WithContext(ctx).Where("id = ?", id).Delete(&MigrationRecord{}).Error; err != nil {
		return fmt.Errorf("failed to remove migration %s: %w", versionString(id), err)
	}
	return nil
}

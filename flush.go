package dbro

import (
	"fmt"

	"gorm.io/gorm"
)

// FlushSQLiteTables flushes all tables in SQLite database
func FlushSQLiteTables(db *gorm.DB) error {
	// Disable foreign key constraints
	if err := db.Exec("PRAGMA foreign_keys = OFF").Error; err != nil {
		return fmt.Errorf("failed to disable foreign keys: %w", err)
	}

	// Get all table names
	var tables []string
	if err := db.Raw("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables).Error; err != nil {
		return fmt.Errorf("failed to get table names: %w", err)
	}

	// Delete all records from each table
	for _, table := range tables {
		if err := db.Exec(fmt.Sprintf("DELETE FROM %s", table)).Error; err != nil {
			return fmt.Errorf("failed to flush table %s: %w", table, err)
		}
	}

	// Re-enable foreign key constraints
	if err := db.Exec("PRAGMA foreign_keys = ON").Error; err != nil {
		return fmt.Errorf("failed to re-enable foreign keys: %w", err)
	}

	return nil
}

// FlushMySQLTables flushes all tables in MySQL database
func FlushMySQLTables(db *gorm.DB) error {
	// Disable foreign key checks
	if err := db.Exec("SET FOREIGN_KEY_CHECKS = 0").Error; err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %w", err)
	}

	// Get all table names
	var tables []string
	if err := db.Raw("SHOW TABLES").Scan(&tables).Error; err != nil {
		return fmt.Errorf("failed to get table names: %w", err)
	}

	// Truncate all tables (faster than DELETE)
	for _, table := range tables {
		if err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)).Error; err != nil {
			return fmt.Errorf("failed to flush table %s: %w", table, err)
		}
	}

	// Re-enable foreign key checks
	if err := db.Exec("SET FOREIGN_KEY_CHECKS = 1").Error; err != nil {
		return fmt.Errorf("failed to re-enable foreign key checks: %w", err)
	}

	return nil
}

// FlushPostgresTables flushes all tables in PostgreSQL database
func FlushPostgresTables(db *gorm.DB) error {
	// Disable all triggers (including foreign key constraints)
	if err := db.Exec("SET session_replication_role = 'replica'").Error; err != nil {
		return fmt.Errorf("failed to disable triggers: %w", err)
	}

	// Get all table names from public schema
	var tables []string
	if err := db.Raw("SELECT tablename FROM pg_tables WHERE schemaname = 'public'").Scan(&tables).Error; err != nil {
		return fmt.Errorf("failed to get table names: %w", err)
	}

	// Truncate all tables with CASCADE to handle any remaining constraints
	for _, table := range tables {
		if err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table)).Error; err != nil {
			return fmt.Errorf("failed to flush table %s: %w", table, err)
		}
	}

	// Re-enable triggers
	if err := db.Exec("SET session_replication_role = 'origin'").Error; err != nil {
		return fmt.Errorf("failed to re-enable triggers: %w", err)
	}

	return nil
}

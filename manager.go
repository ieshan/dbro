package dbro

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"gorm.io/gorm"
)

const (
	DbMySQL    = "mysql"
	DbPostgres = "postgres"
	DbSqlite   = "sqlite"
	DbLibSQL   = "libsql"
)

type connectionFn func(dsn string) (*gorm.DB, error)

type connDsn struct {
	DriverName string
	Dsn        string
}

type ConnectionManager struct {
	connConfigs   map[string]connDsn
	connectionFns map[string]connectionFn
	connections   map[string]*gorm.DB
	mu            sync.RWMutex
	// Migration tracking for RunMigrationOnce
	executedMigrations map[string]struct{}
	migrationMu        sync.RWMutex
}

func (m *ConnectionManager) AddConnectionFunc(driverName string, f connectionFn) {
	m.connectionFns[driverName] = f
}

func (m *ConnectionManager) SetDsn(name, driverName, dsn string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connConfigs[name] = connDsn{
		DriverName: driverName,
		Dsn:        dsn,
	}
}

func (m *ConnectionManager) GetConnection(name string) (*gorm.DB, error) {
	var err error

	m.mu.RLock()
	config, exists := m.connConfigs[name]
	if !exists {
		m.mu.RUnlock()
		return nil, fmt.Errorf("database connection config not found for %s", name)
	}
	connFn, exists := m.connectionFns[config.DriverName]
	if !exists {
		m.mu.RUnlock()
		return nil, fmt.Errorf("database connection function not found for driver %s", config.DriverName)
	}
	conn, exists := m.connections[name]
	m.mu.RUnlock()

	if !exists {
		// Need to create a new connection state
		m.mu.Lock()
		// Double-check pattern: another goroutine might have created it while we were waiting for the lock
		conn, exists = m.connections[name]
		if !exists {
			conn, err = connFn(config.Dsn)
			if err != nil {
				m.mu.Unlock()
				return nil, err
			}
			m.connections[name] = conn
		}
		m.mu.Unlock()
	}
	return conn, nil
}

func (m *ConnectionManager) Close(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	conn, exists := m.connections[name]
	if !exists {
		return fmt.Errorf("connection %s not found", name)
	}
	if conn == nil {
		return fmt.Errorf("connection was not established")
	}
	sqlDB, err := conn.DB()
	if err != nil {
		return err
	}
	if err = sqlDB.Close(); err != nil {
		return err
	}
	delete(m.connections, name)
	return nil
}

func (m *ConnectionManager) CloseAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, conn := range m.connections {
		if conn != nil {
			sqlDB, err := conn.DB()
			if err != nil {
				return err
			}
			if err = sqlDB.Close(); err != nil {
				return err
			}
		}
	}
	m.connections = make(map[string]*gorm.DB)
	return nil
}

// FlushAllTables deletes all records from all tables in the database, ignoring foreign key constraints
func (m *ConnectionManager) FlushAllTables(name string) error {
	db, err := m.GetConnection(name)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	m.mu.RLock()
	config, exists := m.connConfigs[name]
	m.mu.RUnlock()
	if !exists {
		return fmt.Errorf("database connection config not found for %s", name)
	}

	switch config.DriverName {
	case DbSqlite, DbLibSQL:
		return FlushSQLiteTables(db)
	case DbMySQL:
		return FlushMySQLTables(db)
	case DbPostgres:
		return FlushPostgresTables(db)
	default:
		return fmt.Errorf("unsupported database driver: %s", config.DriverName)
	}
}

// DropAllTables drops all tables in the database, ignoring foreign key constraints
func (m *ConnectionManager) DropAllTables(name string) error {
	db, err := m.GetConnection(name)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	m.mu.RLock()
	config, exists := m.connConfigs[name]
	m.mu.RUnlock()
	if !exists {
		return fmt.Errorf("database connection config not found for %s", name)
	}

	switch config.DriverName {
	case DbSqlite, DbLibSQL:
		return DropSQLiteTables(db)
	case DbMySQL:
		return DropMySQLTables(db)
	case DbPostgres:
		return DropPostgresTables(db)
	default:
		return fmt.Errorf("unsupported database driver: %s", config.DriverName)
	}
}

// RunMigration loads and executes SQL migration file
func (m *ConnectionManager) RunMigration(name, filePath string) error {
	// Get database connection
	db, err := m.GetConnection(name)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	// Read SQL file
	sqlContent, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file %s: %w", filePath, err)
	}

	// Convert to string and clean up
	sqlString := strings.TrimSpace(string(sqlContent))
	if sqlString == "" {
		return fmt.Errorf("SQL file %s is empty", filePath)
	}

	// Split SQL content into individual statements
	statements := splitSQLStatements(sqlString)
	if len(statements) == 0 {
		return fmt.Errorf("no valid SQL statements found in file %s", filePath)
	}

	// Execute statements in a transaction for atomicity
	return db.Transaction(func(tx *gorm.DB) error {
		for i, statement := range statements {
			statement = strings.TrimSpace(statement)
			if statement == "" {
				continue // Skip empty statements
			}

			if err := tx.Exec(statement).Error; err != nil {
				return fmt.Errorf("failed to execute statement %d in file %s: %w\nStatement: %s", i+1, filePath, err, statement)
			}
		}
		return nil
	})
}

// RunMigrationOnce loads and executes SQL migration file only once for the given combination
// of dbType, dsn, and filePath. Subsequent calls with the same parameters will be no-op.
func (m *ConnectionManager) RunMigrationOnce(name, filePath string) error {
	// Create a unique key for this migration combination
	migrationKey := fmt.Sprintf("%s:%s", name, filePath)

	// Check if migration was already executed
	m.migrationMu.RLock()
	_, exists := m.executedMigrations[migrationKey]
	m.migrationMu.RUnlock()
	if exists {
		return nil
	}

	// Double-check pattern: another goroutine might have executed it while we were waiting for the lock
	m.migrationMu.Lock()
	defer m.migrationMu.Unlock()

	if _, exists := m.executedMigrations[migrationKey]; exists {
		return nil
	}

	if err := m.RunMigration(name, filePath); err != nil {
		return err
	}
	m.executedMigrations[migrationKey] = struct{}{}
	return nil
}

// Singleton instance and initialization
var (
	instance *ConnectionManager
	once     sync.Once
)

// GetManager returns the singleton instance of ConnectionManager
func GetManager() *ConnectionManager {
	once.Do(func() {
		instance = NewConnectionManager()
	})
	return instance
}

// NewConnectionManager creates a new ConnectionManager instance (for testing or when singleton is not needed)
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connConfigs:        make(map[string]connDsn),
		connectionFns:      make(map[string]connectionFn),
		connections:        make(map[string]*gorm.DB),
		mu:                 sync.RWMutex{},
		executedMigrations: make(map[string]struct{}),
		migrationMu:        sync.RWMutex{},
	}
}

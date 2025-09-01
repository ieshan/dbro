package dbro

import (
	"strings"
)

// splitSQLStatements splits SQL content into individual statements
// This handles basic SQL statement separation by semicolons
func splitSQLStatements(sqlContent string) []string {
	var statements []string

	// Simple but effective approach
	parts := strings.Split(sqlContent, ";")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Skip comments
		if strings.HasPrefix(part, "--") || strings.HasPrefix(part, "#") {
			continue
		}

		statements = append(statements, part)
	}

	return statements
}

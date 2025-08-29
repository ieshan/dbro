package dbro

import (
	"strings"
)

// splitSQLStatements splits SQL content into individual statements
// This handles basic SQL statement separation by semicolons
func splitSQLStatements(sqlContent string) []string {
	var statements []string
	var currentStatement strings.Builder
	var inQuotes bool
	var quoteChar rune

	lines := strings.Split(sqlContent, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip comments
		if strings.HasPrefix(line, "--") || strings.HasPrefix(line, "#") {
			continue
		}

		// Skip SQL comments
		if strings.HasPrefix(line, "/*") {
			continue
		}

		if line == "" {
			continue
		}

		runes := []rune(line)
		for i := 0; i < len(runes); i++ {
			char := runes[i]

			switch {
			case !inQuotes && (char == '\'' || char == '"'):
				inQuotes = true
				quoteChar = char
				currentStatement.WriteRune(char)
			case inQuotes && char == quoteChar:
				// Check if it's an escaped quote (doubled quote)
				if i+1 < len(runes) && runes[i+1] == quoteChar {
					// It's an escaped quote, write both characters
					currentStatement.WriteRune(char)
					currentStatement.WriteRune(char)
					i++ // Skip the next character
				} else {
					// End of quoted string
					inQuotes = false
					currentStatement.WriteRune(char)
				}
			case !inQuotes && char == ';':
				// End of statement
				statement := strings.TrimSpace(currentStatement.String())
				if statement != "" {
					statements = append(statements, statement)
				}
				currentStatement.Reset()
			default:
				currentStatement.WriteRune(char)
			}
		}

		// Add newline if we're in the middle of a statement
		if currentStatement.Len() > 0 {
			currentStatement.WriteRune('\n')
		}
	}

	// Add the last statement if it doesn't end with semicolon
	if currentStatement.Len() > 0 {
		statement := strings.TrimSpace(currentStatement.String())
		if statement != "" {
			statements = append(statements, statement)
		}
	}

	return statements
}

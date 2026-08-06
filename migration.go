package dbro

import (
	"fmt"
	"strings"
	"time"
)

// ParsedMigration represents the result of parsing a migration SQL file.
type ParsedMigration struct {
	UpStatements   []string
	DownStatements []string
	UseTransaction bool
}

// ParseMigration parses SQL content into up/down statement blocks.
//
// Parsing rules:
//   - Only comment lines trigger directive parsing. A comment line begins with one or more '-'.
//   - Within a comment line, scan for tokens: migrate:up, migrate:down, transaction:true, transaction:false.
//   - transaction:* may appear before or after migrate:*, separated by any whitespace.
//   - Once migrate:up is seen, all subsequent non-comment, non-empty lines belong to the up block.
//   - Once migrate:down is seen, all subsequent non-comment, non-empty lines belong to the down block.
//   - Missing migrate:up → UpStatements empty. Missing migrate:down → DownStatements empty.
//   - No transaction:* token → UseTransaction defaults to false.
//   - Genuine SQL comments without directive tokens are ignored.
//   - Multi-line statements can be wrapped with -- statement:begin / -- statement:end as an escape hatch.
func ParseMigration(sqlContent string) (*ParsedMigration, error) {
	result := &ParsedMigration{UseTransaction: false}

	type blockState int
	const (
		stateNone blockState = iota
		stateUp
		stateDown
	)

	currentBlock := stateNone
	inStatementBlock := false
	var currentStmt strings.Builder

	flush := func() {
		s := strings.TrimSpace(currentStmt.String())
		if s == "" {
			return
		}
		switch currentBlock {
		case stateUp:
			result.UpStatements = append(result.UpStatements, s)
		case stateDown:
			result.DownStatements = append(result.DownStatements, s)
		}
		currentStmt.Reset()
	}

	for _, line := range strings.Split(sqlContent, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		if isCommentLine(trimmed) {
			commentBody := extractCommentBody(trimmed)
			for _, tok := range strings.Fields(commentBody) {
				switch tok {
				case "transaction:true":
					result.UseTransaction = true
				case "transaction:false":
					result.UseTransaction = false
				case "migrate:up":
					flush()
					currentBlock = stateUp
				case "migrate:down":
					flush()
					currentBlock = stateDown
				case "statement:begin":
					inStatementBlock = true
				case "statement:end":
					inStatementBlock = false
					flush()
				}
			}
			continue
		}

		if currentBlock == stateNone {
			continue
		}

		if inStatementBlock {
			if currentStmt.Len() > 0 {
				currentStmt.WriteString("\n")
			}
			currentStmt.WriteString(line)
			continue
		}

		// Outside a statement block: accumulate and split by semicolons
		currentStmt.WriteString(line)
		currentStmt.WriteString("\n")

		content := currentStmt.String()
		for strings.Contains(content, ";") {
			idx := strings.Index(content, ";")
			stmt := strings.TrimSpace(content[:idx])
			if stmt != "" {
				if currentBlock == stateUp {
					result.UpStatements = append(result.UpStatements, stmt)
				} else {
					result.DownStatements = append(result.DownStatements, stmt)
				}
			}
			content = content[idx+1:]
		}
		currentStmt.Reset()
		currentStmt.WriteString(content)
	}
	flush()

	return result, nil
}

// isCommentLine reports whether the trimmed line is a SQL comment.
func isCommentLine(trimmed string) bool {
	return strings.HasPrefix(trimmed, "-")
}

// extractCommentBody returns the text after the leading dashes on a comment line.
func extractCommentBody(trimmed string) string {
	i := 0
	for i < len(trimmed) && trimmed[i] == '-' {
		i++
	}
	return strings.TrimSpace(trimmed[i:])
}

// splitSQLStatements splits SQL content into individual statements.
// If inStatementBlock is true, the entire content is treated as a single statement.
func splitSQLStatements(sql string, inStatementBlock bool) []string {
	if inStatementBlock {
		s := strings.TrimSpace(sql)
		if s == "" {
			return nil
		}
		return []string{s}
	}

	var statements []string
	parts := strings.Split(sql, ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Skip comment-only lines
		lines := strings.Split(part, "\n")
		var cleaned []string
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "#") {
				continue
			}
			cleaned = append(cleaned, line)
		}
		if len(cleaned) == 0 {
			continue
		}
		statements = append(statements, strings.Join(cleaned, "\n"))
	}
	return statements
}

// parseMigrationFileName extracts the version timestamp and description from a migration filename.
// Expected format: yyyy-mm-dd-hh-mm-description.sql
func parseMigrationFileName(fileName string) (version time.Time, description string, err error) {
	if !strings.HasSuffix(fileName, ".sql") {
		return time.Time{}, "", fmt.Errorf("migration file must have .sql extension: %s", fileName)
	}
	base := strings.TrimSuffix(fileName, ".sql")

	parts := strings.SplitN(base, "-", 6)
	if len(parts) < 6 {
		return time.Time{}, "", fmt.Errorf("migration filename does not match expected format yyyy-mm-dd-hh-mm-description: %s", fileName)
	}

	versionStr := strings.Join(parts[0:5], "-")
	ver, err := versionFromString(versionStr)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid timestamp in filename %s: %w", fileName, err)
	}

	description = strings.Join(parts[5:], "-")
	return ver, description, nil
}

// versionFromString parses a version string in the format "yyyy-mm-dd-hh-mm".
func versionFromString(s string) (time.Time, error) {
	t, err := time.Parse("2006-01-02-15-04", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid version string %q: %w", s, err)
	}
	return t, nil
}

// versionString formats a time.Time as "yyyy-mm-dd-hh-mm".
func versionString(t time.Time) string {
	return t.Format("2006-01-02-15-04")
}

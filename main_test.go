package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// process is a helper for backward compatibility in tests
func process(path string, sqlfiles *[]string) error {
	return processWithWriter(path, sqlfiles, io.Discard, false)
}

// executeSQL is a helper for backward compatibility in tests
func executeSQL(executor DatabaseExecutor, path string) error {
	return executeSQLWithWriter(executor, path, io.Discard, false)
}

func TestProcess(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "dbversion-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test SQL files
	sql1 := filepath.Join(tmpDir, "test1.sql")
	sql2 := filepath.Join(tmpDir, "test2.sql")

	if err := os.WriteFile(sql1, []byte("SELECT 1;"), 0644); err != nil {
		t.Fatalf("Failed to create test1.sql: %v", err)
	}
	if err := os.WriteFile(sql2, []byte("SELECT 2;"), 0644); err != nil {
		t.Fatalf("Failed to create test2.sql: %v", err)
	}

	// Create index.lst file
	indexPath := filepath.Join(tmpDir, "index.lst")
	indexContent := `# Test index file
test1.sql
test2.sql
`
	if err := os.WriteFile(indexPath, []byte(indexContent), 0644); err != nil {
		t.Fatalf("Failed to create index.lst: %v", err)
	}

	// Process the index file
	var sqlfiles []string
	err = process(indexPath, &sqlfiles)
	if err != nil {
		t.Fatalf("process() failed: %v", err)
	}

	// Verify we got both SQL files
	if len(sqlfiles) != 2 {
		t.Errorf("Expected 2 SQL files, got %d", len(sqlfiles))
	}

	// Verify the files are in the correct order
	expectedFiles := []string{sql1, sql2}
	for i, expected := range expectedFiles {
		if i >= len(sqlfiles) {
			t.Errorf("Missing file at index %d", i)
			continue
		}
		if sqlfiles[i] != expected {
			t.Errorf("Expected file %s at index %d, got %s", expected, i, sqlfiles[i])
		}
	}
}

func TestProcessRecursive(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "dbversion-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create subdirectory
	subDir := filepath.Join(tmpDir, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	// Create SQL files
	sql1 := filepath.Join(tmpDir, "test1.sql")
	sql2 := filepath.Join(subDir, "test2.sql")

	if err := os.WriteFile(sql1, []byte("SELECT 1;"), 0644); err != nil {
		t.Fatalf("Failed to create test1.sql: %v", err)
	}
	if err := os.WriteFile(sql2, []byte("SELECT 2;"), 0644); err != nil {
		t.Fatalf("Failed to create test2.sql: %v", err)
	}

	// Create sub index.lst
	subIndexPath := filepath.Join(subDir, "index.lst")
	if err := os.WriteFile(subIndexPath, []byte("test2.sql\n"), 0644); err != nil {
		t.Fatalf("Failed to create sub index.lst: %v", err)
	}

	// Create main index.lst that references the sub index
	indexPath := filepath.Join(tmpDir, "index.lst")
	indexContent := `test1.sql
subdir/index.lst
`
	if err := os.WriteFile(indexPath, []byte(indexContent), 0644); err != nil {
		t.Fatalf("Failed to create index.lst: %v", err)
	}

	// Process the index file
	var sqlfiles []string
	err = process(indexPath, &sqlfiles)
	if err != nil {
		t.Fatalf("process() failed: %v", err)
	}

	// Verify we got both SQL files
	if len(sqlfiles) != 2 {
		t.Errorf("Expected 2 SQL files, got %d", len(sqlfiles))
	}

	// Verify the files are in the correct order
	if len(sqlfiles) >= 1 && sqlfiles[0] != sql1 {
		t.Errorf("Expected first file to be %s, got %s", sql1, sqlfiles[0])
	}
	if len(sqlfiles) >= 2 && sqlfiles[1] != sql2 {
		t.Errorf("Expected second file to be %s, got %s", sql2, sqlfiles[1])
	}
}

func TestProcessCommentsAndEmptyLines(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbversion-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test SQL file
	sql1 := filepath.Join(tmpDir, "test1.sql")
	if err := os.WriteFile(sql1, []byte("SELECT 1;"), 0644); err != nil {
		t.Fatalf("Failed to create test1.sql: %v", err)
	}

	// Create index.lst with comments and empty lines
	indexPath := filepath.Join(tmpDir, "index.lst")
	indexContent := `# This is a comment

test1.sql
# Another comment

`
	if err := os.WriteFile(indexPath, []byte(indexContent), 0644); err != nil {
		t.Fatalf("Failed to create index.lst: %v", err)
	}

	// Process the index file
	var sqlfiles []string
	err = process(indexPath, &sqlfiles)
	if err != nil {
		t.Fatalf("process() failed: %v", err)
	}

	// Should only have 1 SQL file (comments and empty lines ignored)
	if len(sqlfiles) != 1 {
		t.Errorf("Expected 1 SQL file, got %d", len(sqlfiles))
	}
}

func TestCreateExecutor(t *testing.T) {
	tests := []struct {
		name        string
		engine      DbEngine
		expectError bool
	}{
		{"ClickHouse", ClickHouse, false},
		{"Invalid", DbEngine("invalid"), true},
		{"Postgres (deprecated)", DbEngine("postgres"), true},
		{"MySQL (unsupported)", DbEngine("mysql"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor, err := createExecutor(tt.engine)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for engine %s, got nil", tt.engine)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for engine %s: %v", tt.engine, err)
				}
				if executor == nil {
					t.Errorf("Expected non-nil executor for engine %s", tt.engine)
				}
			}
		})
	}
}

func TestDefaultPorts(t *testing.T) {
	executor, err := createExecutor(ClickHouse)
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	port := executor.DefaultPort()
	expectedPort := 9000
	if port != expectedPort {
		t.Errorf("Expected default port %d for ClickHouse, got %d", expectedPort, port)
	}
}

// MockExecutor for testing executeSQL function
type MockExecutor struct {
	executedSQL []string
	shouldError bool
}

func (m *MockExecutor) Connect(host string, port int, database string, username string, password string, debug bool) error {
	return nil
}

func (m *MockExecutor) Execute(ctx context.Context, sql string) error {
	if m.shouldError {
		return context.DeadlineExceeded
	}
	m.executedSQL = append(m.executedSQL, sql)
	return nil
}

func (m *MockExecutor) Query(ctx context.Context, sql string) ([]map[string]interface{}, error) {
	if m.shouldError {
		return nil, context.DeadlineExceeded
	}
	return nil, nil
}

func (m *MockExecutor) Close() error {
	return nil
}

func (m *MockExecutor) DefaultPort() int {
	return 9000
}

func TestExecuteSQL(t *testing.T) {
	// Create a temporary SQL file
	tmpDir, err := os.MkdirTemp("", "dbversion-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sqlFile := filepath.Join(tmpDir, "test.sql")
	sqlContent := "CREATE TABLE test (id INT);"
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0644); err != nil {
		t.Fatalf("Failed to create test.sql: %v", err)
	}

	// Test successful execution
	mock := &MockExecutor{}
	err = executeSQL(mock, sqlFile)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(mock.executedSQL) != 1 {
		t.Errorf("Expected 1 SQL execution, got %d", len(mock.executedSQL))
	}

	// splitSQLStatements strips the trailing semicolon
	expectedSQL := "CREATE TABLE test (id INT)"
	if len(mock.executedSQL) > 0 && mock.executedSQL[0] != expectedSQL {
		t.Errorf("Expected SQL %q, got %q", expectedSQL, mock.executedSQL[0])
	}

	// Test error handling
	mockError := &MockExecutor{shouldError: true}
	err = executeSQL(mockError, sqlFile)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

// ============================================================================
// Tests for RunConfig and DefaultRunConfig
// ============================================================================

func TestDefaultRunConfig(t *testing.T) {
	cfg := DefaultRunConfig()

	if cfg.Stdout != os.Stdout {
		t.Error("expected Stdout to be os.Stdout")
	}
	if cfg.Stderr != os.Stderr {
		t.Error("expected Stderr to be os.Stderr")
	}
	if cfg.Stdin != os.Stdin {
		t.Error("expected Stdin to be os.Stdin")
	}
	if cfg.Engine != "clickhouse" {
		t.Errorf("expected Engine to be 'clickhouse', got %q", cfg.Engine)
	}
	if cfg.Host != "localhost" {
		t.Errorf("expected Host to be 'localhost', got %q", cfg.Host)
	}
	if cfg.Port != 0 {
		t.Errorf("expected Port to be 0, got %d", cfg.Port)
	}
	if cfg.User != "default" {
		t.Errorf("expected User to be 'default', got %q", cfg.User)
	}
	if cfg.Password != "" {
		t.Error("expected Password to be empty")
	}
	if cfg.Database != "default" {
		t.Errorf("expected Database to be 'default', got %q", cfg.Database)
	}
	if cfg.Path != "./index.lst" {
		t.Errorf("expected Path to be './index.lst', got %q", cfg.Path)
	}
	if cfg.DataPath != "" {
		t.Error("expected DataPath to be empty")
	}
	if cfg.ShowVersion {
		t.Error("expected ShowVersion to be false")
	}
	if cfg.Force {
		t.Error("expected Force to be false")
	}
	if cfg.Debug {
		t.Error("expected Debug to be false")
	}
	if cfg.SkipPassword {
		t.Error("expected SkipPassword to be false")
	}
}

func TestRunConfigStruct(t *testing.T) {
	var stdout, stderr bytes.Buffer

	cfg := RunConfig{
		Stdout:       &stdout,
		Stderr:       &stderr,
		Engine:       "clickhouse",
		Host:         "db.example.com",
		Port:         9001,
		User:         "admin",
		Password:     "",
		Database:     "testdb",
		Path:         "/path/to/index.lst",
		DataPath:     "/path/to/data",
		ShowVersion:  true,
		Force:        true,
		Debug:        true,
		SkipPassword: true,
	}

	if cfg.Host != "db.example.com" {
		t.Error("Host not set correctly")
	}
	if cfg.Port != 9001 {
		t.Error("Port not set correctly")
	}
	if cfg.User != "admin" {
		t.Error("User not set correctly")
	}
	if cfg.Password != "" {
		t.Error("Password not set correctly")
	}
	if cfg.Database != "testdb" {
		t.Error("Database not set correctly")
	}
	if !cfg.ShowVersion {
		t.Error("ShowVersion not set correctly")
	}
	if !cfg.Force {
		t.Error("Force not set correctly")
	}
	if !cfg.Debug {
		t.Error("Debug not set correctly")
	}
	if !cfg.SkipPassword {
		t.Error("SkipPassword not set correctly")
	}
}

// ============================================================================
// Tests for parseFlags
// ============================================================================

func TestParseFlagsDefaults(t *testing.T) {
	var stdout, stderr bytes.Buffer

	cfg := DefaultRunConfig()
	cfg.Stdout = &stdout
	cfg.Stderr = &stderr

	fs, err := parseFlags([]string{}, &cfg)
	if err != nil {
		t.Fatalf("parseFlags failed: %v", err)
	}

	if fs.NFlag() != 0 {
		t.Errorf("expected 0 flags parsed, got %d", fs.NFlag())
	}

	// Values should remain at defaults
	if cfg.Engine != "clickhouse" {
		t.Errorf("expected Engine to be 'clickhouse', got %q", cfg.Engine)
	}
}

func TestParseFlagsWithValues(t *testing.T) {
	var stdout, stderr bytes.Buffer

	cfg := DefaultRunConfig()
	cfg.Stdout = &stdout
	cfg.Stderr = &stderr

	args := []string{
		"-e", "clickhouse",
		"-h", "db.example.com",
		"-p", "9001",
		"-U", "admin",
		"-W", "",
		"-db", "testdb",
		"-path", "/path/to/index.lst",
		"-data", "/path/to/data",
		"-version",
		"-force",
		"-debug",
	}

	_, err := parseFlags(args, &cfg)
	if err != nil {
		t.Fatalf("parseFlags failed: %v", err)
	}

	if cfg.Engine != "clickhouse" {
		t.Errorf("expected Engine 'clickhouse', got %q", cfg.Engine)
	}
	if cfg.Host != "db.example.com" {
		t.Errorf("expected Host 'db.example.com', got %q", cfg.Host)
	}
	if cfg.Port != 9001 {
		t.Errorf("expected Port 9001, got %d", cfg.Port)
	}
	if cfg.User != "admin" {
		t.Errorf("expected User 'admin', got %q", cfg.User)
	}
	if cfg.Password != "" {
		t.Errorf("expected Password 'secret', got %q", cfg.Password)
	}
	if cfg.Database != "testdb" {
		t.Errorf("expected Database 'testdb', got %q", cfg.Database)
	}
	if cfg.Path != "/path/to/index.lst" {
		t.Errorf("expected Path '/path/to/index.lst', got %q", cfg.Path)
	}
	if cfg.DataPath != "/path/to/data" {
		t.Errorf("expected DataPath '/path/to/data', got %q", cfg.DataPath)
	}
	if !cfg.ShowVersion {
		t.Error("expected ShowVersion to be true")
	}
	if !cfg.Force {
		t.Error("expected Force to be true")
	}
	if !cfg.Debug {
		t.Error("expected Debug to be true")
	}
}

func TestParseFlagsInvalidFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer

	cfg := DefaultRunConfig()
	cfg.Stdout = &stdout
	cfg.Stderr = &stderr

	_, err := parseFlags([]string{"-invalid"}, &cfg)
	if err == nil {
		t.Error("expected error for invalid flag")
	}
}

func TestParseFlagsHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer

	cfg := DefaultRunConfig()
	cfg.Stdout = &stdout
	cfg.Stderr = &stderr

	_, err := parseFlags([]string{"-help"}, &cfg)
	if err != flag.ErrHelp {
		t.Errorf("expected flag.ErrHelp, got %v", err)
	}
}

// ============================================================================
// Tests for run() function
// ============================================================================

func TestRunNoArgsShowsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer

	cfg := RunConfig{
		Stdout:       &stdout,
		Stderr:       &stderr,
		Args:         []string{},
		SkipPassword: true,
	}

	exitCode := run(cfg)

	if exitCode != 0 {
		t.Errorf("expected exit code 0, got %d", exitCode)
	}

	output := stderr.String()
	if !strings.Contains(output, "Usage:") {
		t.Errorf("expected usage information in output, got %q", output)
	}
}

func TestRunHelpFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer

	cfg := RunConfig{
		Stdout:       &stdout,
		Stderr:       &stderr,
		Args:         []string{"-help"},
		SkipPassword: true,
	}

	exitCode := run(cfg)

	if exitCode != 0 {
		t.Errorf("expected exit code 0 for help, got %d", exitCode)
	}
}

func TestRunInvalidEngine(t *testing.T) {
	var stdout, stderr bytes.Buffer

	cfg := RunConfig{
		Stdout:       &stdout,
		Stderr:       &stderr,
		Args:         []string{"-e", "invalid"},
		SkipPassword: true,
	}

	exitCode := run(cfg)

	if exitCode != 1 {
		t.Errorf("expected exit code 1, got %d", exitCode)
	}

	errOutput := stderr.String()
	if !strings.Contains(errOutput, "unsupported database engine") {
		t.Errorf("expected error about unsupported engine, got %q", errOutput)
	}
}

func TestRunInvalidFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer

	cfg := RunConfig{
		Stdout: &stdout,
		Stderr: &stderr,
		Args:   []string{"-invalid-flag"},
	}

	exitCode := run(cfg)

	if exitCode != 1 {
		t.Errorf("expected exit code 1, got %d", exitCode)
	}
}

// ============================================================================
// Tests for UsageWriter
// ============================================================================

func TestUsageWriter(t *testing.T) {
	var buf bytes.Buffer

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.String("e", "clickhouse", "Database engine")

	UsageWriter(&buf, "dbmigrate", fs)

	output := buf.String()

	if !strings.Contains(output, "Usage:") {
		t.Error("expected 'Usage:' in output")
	}
	if !strings.Contains(output, "dbmigrate") {
		t.Error("expected 'dbmigrate' in output")
	}
	if !strings.Contains(output, "Examples:") {
		t.Error("expected 'Examples:' in output")
	}
}

// ============================================================================
// Tests for splitSQLStatements
// ============================================================================

func TestSplitSQLStatementsSimple(t *testing.T) {
	sql := "SELECT 1; SELECT 2; SELECT 3;"
	stmts := splitSQLStatements(sql)

	if len(stmts) != 3 {
		t.Errorf("expected 3 statements, got %d", len(stmts))
	}

	expected := []string{"SELECT 1", "SELECT 2", "SELECT 3"}
	for i, exp := range expected {
		if i < len(stmts) && stmts[i] != exp {
			t.Errorf("statement %d: expected %q, got %q", i, exp, stmts[i])
		}
	}
}

func TestSplitSQLStatementsWithSingleQuotes(t *testing.T) {
	sql := "INSERT INTO test VALUES ('hello; world'); SELECT 1;"
	stmts := splitSQLStatements(sql)

	if len(stmts) != 2 {
		t.Errorf("expected 2 statements, got %d: %v", len(stmts), stmts)
	}

	if len(stmts) > 0 && !strings.Contains(stmts[0], "hello; world") {
		t.Errorf("expected semicolon inside quotes to be preserved, got %q", stmts[0])
	}
}

func TestSplitSQLStatementsWithDoubleQuotes(t *testing.T) {
	sql := `SELECT "column;name" FROM test; SELECT 1;`
	stmts := splitSQLStatements(sql)

	if len(stmts) != 2 {
		t.Errorf("expected 2 statements, got %d: %v", len(stmts), stmts)
	}

	if len(stmts) > 0 && !strings.Contains(stmts[0], "column;name") {
		t.Errorf("expected semicolon inside quotes to be preserved, got %q", stmts[0])
	}
}

func TestSplitSQLStatementsWithLineComments(t *testing.T) {
	sql := `SELECT 1; -- comment; with semicolon
SELECT 2;`
	stmts := splitSQLStatements(sql)

	if len(stmts) != 2 {
		t.Errorf("expected 2 statements, got %d: %v", len(stmts), stmts)
	}
}

func TestSplitSQLStatementsWithBlockComments(t *testing.T) {
	sql := `SELECT 1; /* block; comment */ SELECT 2;`
	stmts := splitSQLStatements(sql)

	if len(stmts) != 2 {
		t.Errorf("expected 2 statements, got %d: %v", len(stmts), stmts)
	}
}

func TestSplitSQLStatementsNoTrailingSemicolon(t *testing.T) {
	sql := "SELECT 1; SELECT 2"
	stmts := splitSQLStatements(sql)

	if len(stmts) != 2 {
		t.Errorf("expected 2 statements, got %d: %v", len(stmts), stmts)
	}
}

func TestSplitSQLStatementsEmpty(t *testing.T) {
	sql := ""
	stmts := splitSQLStatements(sql)

	if len(stmts) != 0 {
		t.Errorf("expected 0 statements, got %d", len(stmts))
	}
}

func TestSplitSQLStatementsWhitespaceOnly(t *testing.T) {
	sql := "   ;   ;   "
	stmts := splitSQLStatements(sql)

	if len(stmts) != 0 {
		t.Errorf("expected 0 statements (all whitespace), got %d: %v", len(stmts), stmts)
	}
}

func TestSplitSQLStatementsEscapedQuotes(t *testing.T) {
	sql := `INSERT INTO test VALUES ('it\'s working'); SELECT 1;`
	stmts := splitSQLStatements(sql)

	if len(stmts) != 2 {
		t.Errorf("expected 2 statements, got %d: %v", len(stmts), stmts)
	}
}

// ============================================================================
// Tests for parseMigrationInfo
// ============================================================================

func TestParseMigrationInfoWithVersionAndDescription(t *testing.T) {
	tmpDir := t.TempDir()
	sqlFile := filepath.Join(tmpDir, "test.sql")

	content := `-- version: 1.0.0
-- description: Initial schema creation
CREATE TABLE test (id INT);
`
	if err := os.WriteFile(sqlFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	info, err := parseMigrationInfo(sqlFile)
	if err != nil {
		t.Fatalf("parseMigrationInfo failed: %v", err)
	}

	if info.Version != "1.0.0" {
		t.Errorf("expected Version '1.0.0', got %q", info.Version)
	}
	if info.Description != "Initial schema creation" {
		t.Errorf("expected Description 'Initial schema creation', got %q", info.Description)
	}
	if info.Filename != "test.sql" {
		t.Errorf("expected Filename 'test.sql', got %q", info.Filename)
	}
	if info.Checksum == "" {
		t.Error("expected Checksum to be set")
	}
}

func TestParseMigrationInfoNoHeaders(t *testing.T) {
	tmpDir := t.TempDir()
	sqlFile := filepath.Join(tmpDir, "test.sql")

	content := "CREATE TABLE test (id INT);"
	if err := os.WriteFile(sqlFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	info, err := parseMigrationInfo(sqlFile)
	if err != nil {
		t.Fatalf("parseMigrationInfo failed: %v", err)
	}

	if info.Version != "" {
		t.Errorf("expected empty Version, got %q", info.Version)
	}
	if info.Description != "" {
		t.Errorf("expected empty Description, got %q", info.Description)
	}
	if info.Checksum == "" {
		t.Error("expected Checksum to be set")
	}
}

func TestParseMigrationInfoFileNotFound(t *testing.T) {
	_, err := parseMigrationInfo("/nonexistent/path/test.sql")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestParseMigrationInfoChecksumConsistent(t *testing.T) {
	tmpDir := t.TempDir()
	sqlFile := filepath.Join(tmpDir, "test.sql")

	content := "CREATE TABLE test (id INT);"
	if err := os.WriteFile(sqlFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	info1, _ := parseMigrationInfo(sqlFile)
	info2, _ := parseMigrationInfo(sqlFile)

	if info1.Checksum != info2.Checksum {
		t.Error("checksum should be consistent for same file")
	}
}

func TestParseMigrationInfoChecksumChanges(t *testing.T) {
	tmpDir := t.TempDir()
	sqlFile := filepath.Join(tmpDir, "test.sql")

	content1 := "CREATE TABLE test (id INT);"
	if err := os.WriteFile(sqlFile, []byte(content1), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	info1, _ := parseMigrationInfo(sqlFile)

	content2 := "CREATE TABLE test (id BIGINT);"
	if err := os.WriteFile(sqlFile, []byte(content2), 0644); err != nil {
		t.Fatalf("failed to update test file: %v", err)
	}
	info2, _ := parseMigrationInfo(sqlFile)

	if info1.Checksum == info2.Checksum {
		t.Error("checksum should change when file content changes")
	}
}

// ============================================================================
// Tests for escapeSQLString
// ============================================================================

func TestEscapeSQLString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "hello"},
		{"it's", "it''s"},
		{"", ""},
		{"don't stop", "don''t stop"},
		{"''", "''''"},
	}

	for _, tt := range tests {
		result := escapeSQLString(tt.input)
		if result != tt.expected {
			t.Errorf("escapeSQLString(%q) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

// ============================================================================
// Tests for applyDefaultsWithConfig
// ============================================================================

func TestApplyDefaultsWithConfigPort(t *testing.T) {
	executor, _ := createExecutor(ClickHouse)
	cfg := &RunConfig{
		Engine: "clickhouse",
		Port:   0,
	}

	applyDefaultsWithConfig(executor, cfg)

	if cfg.Port != 9000 {
		t.Errorf("expected Port 9000, got %d", cfg.Port)
	}
}

func TestApplyDefaultsWithConfigPortPreserved(t *testing.T) {
	executor, _ := createExecutor(ClickHouse)
	cfg := &RunConfig{
		Engine: "clickhouse",
		Port:   9001,
	}

	applyDefaultsWithConfig(executor, cfg)

	if cfg.Port != 9001 {
		t.Errorf("expected Port 9001 to be preserved, got %d", cfg.Port)
	}
}

func TestApplyDefaultsWithConfigClickHouseDefaults(t *testing.T) {
	executor, _ := createExecutor(ClickHouse)
	cfg := &RunConfig{
		Engine:   "clickhouse",
		User:     "",
		Database: "",
	}

	applyDefaultsWithConfig(executor, cfg)

	if cfg.User != "default" {
		t.Errorf("expected User 'default', got %q", cfg.User)
	}
	if cfg.Database != "default" {
		t.Errorf("expected Database 'default', got %q", cfg.Database)
	}
}

// ============================================================================
// Tests for loadCSVData and loadCSVFile
// ============================================================================

func TestLoadCSVFileWithWriter(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")

	content := `id,name,value
1,foo,100
2,bar,200
`
	if err := os.WriteFile(csvFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test CSV: %v", err)
	}

	mock := &MockExecutor{}
	var stdout bytes.Buffer

	err := loadCSVFileWithWriter(mock, csvFile, "test_table", &stdout, false)
	if err != nil {
		t.Fatalf("loadCSVFileWithWriter failed: %v", err)
	}

	if len(mock.executedSQL) != 1 {
		t.Errorf("expected 1 SQL execution, got %d", len(mock.executedSQL))
	}

	sql := mock.executedSQL[0]
	if !strings.Contains(sql, "INSERT INTO test_table") {
		t.Errorf("expected INSERT statement, got %q", sql)
	}
	if !strings.Contains(sql, "id, name, value") {
		t.Errorf("expected column names, got %q", sql)
	}
	if !strings.Contains(sql, "'1'") && !strings.Contains(sql, "'foo'") {
		t.Errorf("expected values, got %q", sql)
	}
}

func TestLoadCSVFileEmptyData(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")

	content := "id,name,value\n"
	if err := os.WriteFile(csvFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test CSV: %v", err)
	}

	mock := &MockExecutor{}
	var stdout bytes.Buffer

	err := loadCSVFileWithWriter(mock, csvFile, "test_table", &stdout, false)
	if err != nil {
		t.Fatalf("loadCSVFileWithWriter failed: %v", err)
	}

	// No SQL should be executed for empty data
	if len(mock.executedSQL) != 0 {
		t.Errorf("expected 0 SQL executions for empty data, got %d", len(mock.executedSQL))
	}
}

func TestLoadCSVFileNotFound(t *testing.T) {
	mock := &MockExecutor{}
	var stdout bytes.Buffer

	err := loadCSVFileWithWriter(mock, "/nonexistent/file.csv", "table", &stdout, false)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadCSVFileWithQuotes(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")

	content := `id,name
1,it's quoted
`
	if err := os.WriteFile(csvFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test CSV: %v", err)
	}

	mock := &MockExecutor{}
	var stdout bytes.Buffer

	err := loadCSVFileWithWriter(mock, csvFile, "test_table", &stdout, false)
	if err != nil {
		t.Fatalf("loadCSVFileWithWriter failed: %v", err)
	}

	sql := mock.executedSQL[0]
	// Single quotes should be escaped
	if !strings.Contains(sql, "it''s quoted") {
		t.Errorf("expected escaped quotes, got %q", sql)
	}
}

func TestLoadCSVDataWithWriter(t *testing.T) {
	tmpDir := t.TempDir()

	// Create two CSV files
	csv1 := filepath.Join(tmpDir, "table1.csv")
	csv2 := filepath.Join(tmpDir, "table2.csv")

	if err := os.WriteFile(csv1, []byte("id,name\n1,foo\n"), 0644); err != nil {
		t.Fatalf("failed to create CSV1: %v", err)
	}
	if err := os.WriteFile(csv2, []byte("id,value\n2,bar\n"), 0644); err != nil {
		t.Fatalf("failed to create CSV2: %v", err)
	}

	mock := &MockExecutor{}
	var stdout bytes.Buffer

	err := loadCSVDataWithWriter(mock, tmpDir, &stdout, false)
	if err != nil {
		t.Fatalf("loadCSVDataWithWriter failed: %v", err)
	}

	if len(mock.executedSQL) != 2 {
		t.Errorf("expected 2 SQL executions, got %d", len(mock.executedSQL))
	}
}

func TestLoadCSVDataEmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	mock := &MockExecutor{}
	var stdout bytes.Buffer

	err := loadCSVDataWithWriter(mock, tmpDir, &stdout, false)
	if err != nil {
		t.Fatalf("loadCSVDataWithWriter failed: %v", err)
	}

	output := stdout.String()
	if !strings.Contains(output, "No CSV files found") {
		t.Errorf("expected 'No CSV files found' message, got %q", output)
	}
}

func TestLoadCSVDataDirectoryNotFound(t *testing.T) {
	mock := &MockExecutor{}
	var stdout bytes.Buffer

	err := loadCSVDataWithWriter(mock, "/nonexistent/directory", &stdout, false)
	if err == nil {
		t.Error("expected error for nonexistent directory")
	}
}

// ============================================================================
// Tests for showSchemaVersionWithWriter
// ============================================================================

func TestShowSchemaVersionWithWriterNoMigrations(t *testing.T) {
	mock := &MockExecutorWithRows{
		rows: []map[string]interface{}{},
	}
	var stdout bytes.Buffer

	showSchemaVersionWithWriter(mock, &stdout)

	output := stdout.String()
	if !strings.Contains(output, "No migrations have been applied") {
		t.Errorf("expected 'No migrations have been applied', got %q", output)
	}
}

func TestShowSchemaVersionWithWriterWithMigrations(t *testing.T) {
	mock := &MockExecutorWithRows{
		rows: []map[string]interface{}{
			{"version": "1.0.0", "applied_at": "2024-01-01", "description": "Initial", "filename": "001.sql"},
			{"version": "0.9.0", "applied_at": "2023-12-01", "description": "Previous", "filename": "000.sql"},
		},
	}
	var stdout bytes.Buffer

	showSchemaVersionWithWriter(mock, &stdout)

	output := stdout.String()
	if !strings.Contains(output, "Current:") {
		t.Error("expected 'Current:' in output")
	}
	if !strings.Contains(output, "1.0.0") {
		t.Error("expected version '1.0.0' in output")
	}
}

func TestShowSchemaVersionWithWriterQueryError(t *testing.T) {
	mock := &MockExecutorWithRows{
		shouldError: true,
	}
	var stdout bytes.Buffer

	showSchemaVersionWithWriter(mock, &stdout)

	output := stdout.String()
	if !strings.Contains(output, "Could not query schema version") {
		t.Errorf("expected error message, got %q", output)
	}
}

// MockExecutorWithRows extends MockExecutor to return rows from Query
type MockExecutorWithRows struct {
	MockExecutor
	rows        []map[string]interface{}
	shouldError bool
}

func (m *MockExecutorWithRows) Query(ctx context.Context, sql string) ([]map[string]interface{}, error) {
	if m.shouldError {
		return nil, errors.New("query error")
	}
	return m.rows, nil
}

// ============================================================================
// Tests for getAppliedMigrations
// ============================================================================

func TestGetAppliedMigrations(t *testing.T) {
	mock := &MockExecutorWithRows{
		rows: []map[string]interface{}{
			{"version": "1.0.0", "checksum": "abc123"},
			{"version": "1.1.0", "checksum": "def456"},
		},
	}

	migrations, err := getAppliedMigrations(mock)
	if err != nil {
		t.Fatalf("getAppliedMigrations failed: %v", err)
	}

	if len(migrations) != 2 {
		t.Errorf("expected 2 migrations, got %d", len(migrations))
	}

	if migrations["1.0.0"] != "abc123" {
		t.Errorf("expected checksum 'abc123' for version 1.0.0, got %q", migrations["1.0.0"])
	}
}

func TestGetAppliedMigrationsError(t *testing.T) {
	mock := &MockExecutorWithRows{
		shouldError: true,
	}

	_, err := getAppliedMigrations(mock)
	if err == nil {
		t.Error("expected error")
	}
}

// ============================================================================
// Tests for recordMigration
// ============================================================================

func TestRecordMigration(t *testing.T) {
	mock := &MockExecutor{}

	info := MigrationInfo{
		Version:     "1.0.0",
		Description: "Test migration",
		Filename:    "001_test.sql",
		Checksum:    "abc123",
	}

	err := recordMigration(mock, info)
	if err != nil {
		t.Fatalf("recordMigration failed: %v", err)
	}

	if len(mock.executedSQL) != 1 {
		t.Errorf("expected 1 SQL execution, got %d", len(mock.executedSQL))
	}

	sql := mock.executedSQL[0]
	if !strings.Contains(sql, "INSERT INTO schema_versions") {
		t.Errorf("expected INSERT statement, got %q", sql)
	}
	if !strings.Contains(sql, "1.0.0") {
		t.Errorf("expected version in SQL, got %q", sql)
	}
}

// ============================================================================
// Tests for ClickHouseExecutor
// ============================================================================

func TestClickHouseExecutorDefaultPort(t *testing.T) {
	executor := &ClickHouseExecutor{}
	port := executor.DefaultPort()
	if port != 9000 {
		t.Errorf("expected default port 9000, got %d", port)
	}
}

func TestClickHouseExecutorClose(t *testing.T) {
	executor := &ClickHouseExecutor{}
	// Close on uninitialized connection should not error
	err := executor.Close()
	if err != nil {
		t.Errorf("expected no error on Close, got %v", err)
	}
}

// ============================================================================
// Tests for DbEngine type
// ============================================================================

func TestDbEngineType(t *testing.T) {
	engine := ClickHouse
	if engine != "clickhouse" {
		t.Errorf("expected 'clickhouse', got %q", engine)
	}

	// Test type conversion
	var e DbEngine = "clickhouse"
	if e != ClickHouse {
		t.Error("DbEngine type conversion failed")
	}
}

// ============================================================================
// Tests for MigrationInfo struct
// ============================================================================

func TestMigrationInfoStruct(t *testing.T) {
	info := MigrationInfo{
		Version:     "2.0.0",
		Description: "Add new tables",
		Filename:    "002_add_tables.sql",
		Checksum:    "xyz789",
	}

	if info.Version != "2.0.0" {
		t.Error("Version not set correctly")
	}
	if info.Description != "Add new tables" {
		t.Error("Description not set correctly")
	}
	if info.Filename != "002_add_tables.sql" {
		t.Error("Filename not set correctly")
	}
	if info.Checksum != "xyz789" {
		t.Error("Checksum not set correctly")
	}
}

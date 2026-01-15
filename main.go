// dbmigrate - Database schema migration tool for ClickHouse
//
// Install: go install github.com/quantumgateway/dbmigrate@latest
//
// Usage:
//
//	dbmigrate -e clickhouse -h localhost -db mydb -path ./sql/index.lst
package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/term"
)

// Version is set at build time
var Version = "dev"

type DbEngine string

const (
	ClickHouse DbEngine = "clickhouse"
)

// MigrationInfo holds metadata extracted from SQL file headers
type MigrationInfo struct {
	Version     string
	Description string
	Filename    string
	Checksum    string
}

var (
	ctxbg = context.Background()

	// Regex patterns for parsing version headers
	versionRegex     = regexp.MustCompile(`^--\s*version:\s*(.+)$`)
	descriptionRegex = regexp.MustCompile(`^--\s*description:\s*(.+)$`)
)

// RunConfig holds configuration for the run function
type RunConfig struct {
	Stdout         io.Writer
	Stderr         io.Writer
	Stdin          io.Reader
	Args           []string
	Engine         string
	Host           string
	Port           int
	User           string
	Password       string
	Database       string
	Path           string
	DataPath       string
	ShowVersion    bool
	Force          bool
	Debug          bool
	SkipPassword   bool // Skip password prompt (for testing)
	PromptPassword bool // -W flag: prompt for password
}

// DefaultRunConfig returns a RunConfig with default values
func DefaultRunConfig() RunConfig {
	return RunConfig{
		Stdout:   os.Stdout,
		Stderr:   os.Stderr,
		Stdin:    os.Stdin,
		Engine:   "clickhouse",
		Host:     "localhost",
		Port:     0,
		User:     "default",
		Password: "",
		Database: "default",
		Path:     "./index.lst",
		DataPath: "",
	}
}

// UsageWriter writes usage information to the provided writer
func UsageWriter(w io.Writer, progName string, fs *flag.FlagSet) {
	fmt.Fprintf(w, "dbmigrate %s - Database schema migration tool for ClickHouse\n\n", Version)
	fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", progName)
	fmt.Fprintf(w, "Options:\n")
	fs.SetOutput(w)
	fs.PrintDefaults()
	fmt.Fprintf(w, "\nExamples:\n")
	fmt.Fprintf(w, "  # Initialize ClickHouse schema with password prompt\n")
	fmt.Fprintf(w, "  %s -e clickhouse -h localhost -U default -W -db default -path ./sql/index.lst\n\n", progName)
	fmt.Fprintf(w, "  # Initialize ClickHouse schema with password on command line\n")
	fmt.Fprintf(w, "  %s -e clickhouse -h localhost -U default -password mypass -db default -path ./sql/index.lst\n\n", progName)
	fmt.Fprintf(w, "  # Show current schema version\n")
	fmt.Fprintf(w, "  %s -e clickhouse -h localhost -db default -version\n\n", progName)
	fmt.Fprintf(w, "  # Force re-run migrations (skip version checks)\n")
	fmt.Fprintf(w, "  %s -e clickhouse -h localhost -db default -path ./sql/index.lst -force\n\n", progName)
	fmt.Fprintf(w, "  # Load test data from CSV files\n")
	fmt.Fprintf(w, "  %s -e clickhouse -h localhost -db default -path ./sql/index.lst -data ./testdata/csv\n", progName)
}

func Usage() {
	UsageWriter(os.Stderr, os.Args[0], flag.CommandLine)
}

// DatabaseExecutor defines the interface for database-specific operations
type DatabaseExecutor interface {
	Connect(host string, port int, database string, username string, password string, debug bool) error
	Execute(ctx context.Context, sql string) error
	Query(ctx context.Context, sql string) ([]map[string]interface{}, error)
	Close() error
	DefaultPort() int
}

// parseFlags parses command line flags into a RunConfig
func parseFlags(args []string, cfg *RunConfig) (*flag.FlagSet, error) {
	fs := flag.NewFlagSet("dbmigrate", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)

	fs.StringVar(&cfg.Engine, "e", cfg.Engine, "Database engine (only clickhouse supported)")
	fs.StringVar(&cfg.Host, "h", cfg.Host, "Hostname of the database server")
	fs.IntVar(&cfg.Port, "p", cfg.Port, "Port the database server listens to (0 = use default: 9000)")
	fs.StringVar(&cfg.User, "U", cfg.User, "Database username")
	fs.BoolVar(&cfg.PromptPassword, "W", cfg.PromptPassword, "Prompt for password")
	fs.StringVar(&cfg.Password, "password", cfg.Password, "Database password (alternative to -W prompt)")
	fs.StringVar(&cfg.Database, "db", cfg.Database, "Database name")
	fs.StringVar(&cfg.Path, "path", cfg.Path, "Path to the index.lst file containing SQL files to execute")
	fs.StringVar(&cfg.DataPath, "data", cfg.DataPath, "Path to directory containing CSV files for test/development data (optional)")
	fs.BoolVar(&cfg.ShowVersion, "version", cfg.ShowVersion, "Show current schema version and exit")
	fs.BoolVar(&cfg.Force, "force", cfg.Force, "Force re-run migrations even if already applied")
	fs.BoolVar(&cfg.Debug, "debug", cfg.Debug, "Enable debug logging")

	fs.Usage = func() {
		UsageWriter(cfg.Stderr, "dbmigrate", fs)
	}

	if err := fs.Parse(args); err != nil {
		return fs, err
	}

	return fs, nil
}

// run is the main entry point logic, separated for testability
func run(cfg RunConfig) int {
	fs, err := parseFlags(cfg.Args, &cfg)
	if err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 1
	}

	// Show usage if no flags provided
	if fs.NFlag() == 0 && len(cfg.Args) == 0 {
		UsageWriter(cfg.Stderr, "dbmigrate", fs)
		return 0
	}

	// Create the appropriate database executor
	executor, err := createExecutor(DbEngine(cfg.Engine))
	if err != nil {
		fmt.Fprintf(cfg.Stderr, "Error creating database executor: %s\n", err)
		return 1
	}
	defer executor.Close()

	// Apply defaults based on engine
	applyDefaultsWithConfig(executor, &cfg)

	// Prompt for password if -W flag is set and password not already provided
	if cfg.PromptPassword && len(cfg.Password) == 0 && !cfg.SkipPassword {
		fmt.Fprintf(cfg.Stdout, "Password for user %s: ", cfg.User)
		bytepw, err := term.ReadPassword(syscall.Stdin)
		fmt.Fprintln(cfg.Stdout) // New line after password input
		if err != nil {
			fmt.Fprintf(cfg.Stderr, "Error reading password: %s\n", err)
			return 1
		}
		cfg.Password = string(bytepw)
	}

	// Connect to database
	err = executor.Connect(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.Debug)
	if err != nil {
		fmt.Fprintf(cfg.Stderr, "Error connecting to database: %s\n", err)
		return 1
	}

	// If -version flag is set, show version and exit
	if cfg.ShowVersion {
		showSchemaVersionWithWriter(executor, cfg.Stdout)
		return 0
	}

	// Get already applied migrations
	appliedMigrations, err := getAppliedMigrations(executor)
	if err != nil {
		// Table might not exist yet, continue with empty map
		if cfg.Debug {
			fmt.Fprintf(cfg.Stdout, "Note: Could not query schema_versions (table may not exist yet): %v\n", err)
		}
		appliedMigrations = make(map[string]string)
	}

	// Process index.lst file to get list of SQL files
	var sqlfiles = &[]string{}
	err = processWithWriter(cfg.Path, sqlfiles, cfg.Stdout, cfg.Debug)
	if err != nil {
		fmt.Fprintf(cfg.Stderr, "Error processing index file: %s\n", err)
		return 1
	}

	// Execute each SQL file
	migrationsApplied := 0
	migrationsSkipped := 0
	for _, sqlFile := range *sqlfiles {
		info, err := parseMigrationInfo(sqlFile)
		if err != nil {
			fmt.Fprintf(cfg.Stdout, "Warning: Could not parse migration info from %s: %v\n", sqlFile, err)
		}

		// Check if migration was already applied
		if !cfg.Force && info.Version != "" {
			if existingChecksum, exists := appliedMigrations[info.Version]; exists {
				if existingChecksum == info.Checksum {
					if cfg.Debug {
						fmt.Fprintf(cfg.Stdout, "Skipping %s (version %s already applied)\n", filepath.Base(sqlFile), info.Version)
					}
					migrationsSkipped++
					continue
				} else {
					fmt.Fprintf(cfg.Stderr, "WARNING: Checksum mismatch for version %s\n", info.Version)
					fmt.Fprintf(cfg.Stderr, "  File: %s\n", sqlFile)
					fmt.Fprintf(cfg.Stderr, "  Expected checksum: %s\n", existingChecksum)
					fmt.Fprintf(cfg.Stderr, "  Current checksum:  %s\n", info.Checksum)
					fmt.Fprintf(cfg.Stderr, "  Use -force to re-apply this migration\n")
					return 1
				}
			}
		}

		err = executeSQLWithWriter(executor, sqlFile, cfg.Stdout, cfg.Debug)
		if err != nil {
			fmt.Fprintf(cfg.Stderr, "Error executing %s: %s\n", sqlFile, err)
			return 1
		}

		// Record the migration if it has version info
		if info.Version != "" {
			err = recordMigration(executor, info)
			if err != nil {
				fmt.Fprintf(cfg.Stdout, "Warning: Could not record migration %s: %v\n", info.Version, err)
			}
		}
		migrationsApplied++
	}

	fmt.Fprintf(cfg.Stdout, "\nMigration complete: %d applied, %d skipped\n", migrationsApplied, migrationsSkipped)

	// Load CSV data if path is provided
	if cfg.DataPath != "" {
		err = loadCSVDataWithWriter(executor, cfg.DataPath, cfg.Stdout, cfg.Debug)
		if err != nil {
			fmt.Fprintf(cfg.Stderr, "Error loading CSV data: %s\n", err)
			return 1
		}
	}

	return 0
}

func main() {
	cfg := DefaultRunConfig()
	cfg.Args = os.Args[1:]
	os.Exit(run(cfg))
}

// showSchemaVersionWithWriter displays the current schema version to the provided writer
func showSchemaVersionWithWriter(executor DatabaseExecutor, w io.Writer) {
	rows, err := executor.Query(ctxbg, `
		SELECT version, applied_at, description, filename
		FROM schema_versions
		ORDER BY applied_at DESC
		LIMIT 10
	`)
	if err != nil {
		fmt.Fprintf(w, "Could not query schema version: %v\n", err)
		fmt.Fprintln(w, "The schema_versions table may not exist yet.")
		return
	}

	if len(rows) == 0 {
		fmt.Fprintln(w, "No migrations have been applied yet.")
		return
	}

	fmt.Fprintln(w, "Schema version history (most recent first):")
	fmt.Fprintln(w, "--------------------------------------------")
	for i, row := range rows {
		version := row["version"]
		appliedAt := row["applied_at"]
		description := row["description"]
		if i == 0 {
			fmt.Fprintf(w, "Current: %v (%v)\n", version, appliedAt)
			fmt.Fprintf(w, "         %v\n", description)
		} else {
			fmt.Fprintf(w, "         %v (%v) - %v\n", version, appliedAt, description)
		}
	}
}

// getAppliedMigrations returns a map of version -> checksum for all applied migrations
func getAppliedMigrations(executor DatabaseExecutor) (map[string]string, error) {
	rows, err := executor.Query(ctxbg, "SELECT version, checksum FROM schema_versions")
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, row := range rows {
		version, _ := row["version"].(string)
		checksum, _ := row["checksum"].(string)
		if version != "" {
			result[version] = checksum
		}
	}
	return result, nil
}

// parseMigrationInfo extracts version and description from SQL file header comments
func parseMigrationInfo(path string) (MigrationInfo, error) {
	info := MigrationInfo{
		Filename: filepath.Base(path),
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return info, err
	}

	// Calculate checksum of entire file
	hash := md5.Sum(content)
	info.Checksum = hex.EncodeToString(hash[:])

	// Parse first few lines for version and description
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	lineCount := 0
	for scanner.Scan() && lineCount < 10 {
		line := scanner.Text()
		lineCount++

		if matches := versionRegex.FindStringSubmatch(line); len(matches) > 1 {
			info.Version = strings.TrimSpace(matches[1])
		}
		if matches := descriptionRegex.FindStringSubmatch(line); len(matches) > 1 {
			info.Description = strings.TrimSpace(matches[1])
		}

		// Stop if we've found both
		if info.Version != "" && info.Description != "" {
			break
		}
	}

	return info, nil
}

// recordMigration inserts a record into schema_versions table
func recordMigration(executor DatabaseExecutor, info MigrationInfo) error {
	sql := fmt.Sprintf(
		"INSERT INTO schema_versions (version, description, filename, checksum) VALUES ('%s', '%s', '%s', '%s')",
		escapeSQLString(info.Version),
		escapeSQLString(info.Description),
		escapeSQLString(info.Filename),
		escapeSQLString(info.Checksum),
	)
	return executor.Execute(ctxbg, sql)
}

// escapeSQLString escapes single quotes for SQL strings
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// applyDefaultsWithConfig sets default values based on the database engine using RunConfig
func applyDefaultsWithConfig(executor DatabaseExecutor, cfg *RunConfig) {
	if cfg.Port == 0 {
		cfg.Port = executor.DefaultPort()
	}

	switch DbEngine(cfg.Engine) {
	case ClickHouse:
		if cfg.User == "" {
			cfg.User = "default"
		}
		if cfg.Database == "" {
			cfg.Database = "default"
		}
	}
}

// createExecutor creates the appropriate database executor based on the engine type
func createExecutor(engine DbEngine) (DatabaseExecutor, error) {
	switch engine {
	case ClickHouse:
		return &ClickHouseExecutor{}, nil
	default:
		return nil, fmt.Errorf("unsupported database engine: %s (only 'clickhouse' is supported)", engine)
	}
}

// ClickHouseExecutor implements DatabaseExecutor for ClickHouse
type ClickHouseExecutor struct {
	conn driver.Conn
}

func (e *ClickHouseExecutor) Connect(host string, port int, database string, username string, password string, debug bool) error {
	url := fmt.Sprintf("%s:%d", host, port)
	var err error
	e.conn, err = clickhouse.Open(&clickhouse.Options{
		MaxOpenConns: 12,
		Addr:         []string{url},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Debug: debug,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	fmt.Printf("Connected to ClickHouse: %s@%s:%d/%s\n", username, host, port, database)
	return nil
}

func (e *ClickHouseExecutor) Execute(ctx context.Context, sql string) error {
	err := e.conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("ClickHouse execution error: %w", err)
	}
	return nil
}

func (e *ClickHouseExecutor) Query(ctx context.Context, sql string) ([]map[string]interface{}, error) {
	rows, err := e.conn.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("ClickHouse query error: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	columns := rows.Columns()

	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		// Create a map for this row
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	return results, nil
}

func (e *ClickHouseExecutor) Close() error {
	if e.conn != nil {
		return e.conn.Close()
	}
	return nil
}

func (e *ClickHouseExecutor) DefaultPort() int {
	return 9000
}

// processWithWriter reads an index.lst file and recursively collects SQL files
func processWithWriter(path string, sqlfiles *[]string, w io.Writer, debug bool) error {
	file, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(w, "Error opening file: %s\n", err)
		return err
	}
	defer file.Close()

	dir := filepath.Dir(file.Name())
	fmt.Fprintf(w, "Processing file: %v in dir: %v\n", file.Name(), dir)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue // Skip empty lines and comments
		}

		fileName := line
		if strings.HasSuffix(fileName, ".lst") {
			err := processWithWriter(filepath.Join(dir, fileName), sqlfiles, w, debug)
			if err != nil {
				return err
			}
		} else if strings.HasSuffix(fileName, ".sql") {
			if debug {
				fmt.Fprintf(w, "Adding SQL file: %v\n", fileName)
			}
			*sqlfiles = append(*sqlfiles, filepath.Join(dir, fileName))
		} else {
			fmt.Fprintf(w, "Warning: unknown file type: %v\n", fileName)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file %s: %w", path, err)
	}

	return nil
}

// executeSQLWithWriter reads and executes a SQL file using the provided executor and writer
func executeSQLWithWriter(executor DatabaseExecutor, path string, w io.Writer, debug bool) error {
	fmt.Fprintf(w, "Executing SQL file: %v\n", path)

	sql, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	// Split SQL content into individual statements
	statements := splitSQLStatements(string(sql))

	// Execute each statement separately
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue // Skip empty statements
		}

		if debug {
			fmt.Fprintf(w, "  Executing statement %d/%d\n", i+1, len(statements))
		}

		err = executor.Execute(ctxbg, stmt)
		if err != nil {
			return fmt.Errorf("statement %d failed: %w", i+1, err)
		}
	}

	fmt.Fprintf(w, "Successfully executed: %v (%d statements)\n", path, len(statements))
	return nil
}

// splitSQLStatements splits SQL content by semicolons while respecting string literals
// and comment blocks to avoid splitting statements incorrectly
func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	var inSingleQuote, inDoubleQuote bool
	var inLineComment, inBlockComment bool

	runes := []rune(sql)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]

		// Check for line comments
		if !inSingleQuote && !inDoubleQuote && !inBlockComment && ch == '-' && i+1 < len(runes) && runes[i+1] == '-' {
			inLineComment = true
			current.WriteRune(ch)
			continue
		}

		// End line comment on newline
		if inLineComment && (ch == '\n' || ch == '\r') {
			inLineComment = false
			current.WriteRune(ch)
			continue
		}

		// Check for block comments
		if !inSingleQuote && !inDoubleQuote && !inLineComment && ch == '/' && i+1 < len(runes) && runes[i+1] == '*' {
			inBlockComment = true
			current.WriteRune(ch)
			continue
		}

		// End block comment
		if inBlockComment && ch == '*' && i+1 < len(runes) && runes[i+1] == '/' {
			inBlockComment = false
			current.WriteRune(ch)
			i++ // Skip the '/'
			current.WriteRune(runes[i])
			continue
		}

		// Toggle quote states
		if !inLineComment && !inBlockComment {
			if ch == '\'' && !inDoubleQuote {
				// Check if it's an escaped quote
				if i > 0 && runes[i-1] == '\\' {
					current.WriteRune(ch)
					continue
				}
				inSingleQuote = !inSingleQuote
			} else if ch == '"' && !inSingleQuote {
				if i > 0 && runes[i-1] == '\\' {
					current.WriteRune(ch)
					continue
				}
				inDoubleQuote = !inDoubleQuote
			}
		}

		// Split on semicolon if not in quotes or comments
		if ch == ';' && !inSingleQuote && !inDoubleQuote && !inLineComment && !inBlockComment {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteRune(ch)
	}

	// Add the last statement if there is one
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// loadCSVDataWithWriter reads CSV files from a directory and loads them into database tables
func loadCSVDataWithWriter(executor DatabaseExecutor, dataDir string, w io.Writer, debug bool) error {
	fmt.Fprintf(w, "Loading CSV data from: %v\n", dataDir)

	// Read directory contents
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	// Process each CSV file
	csvCount := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}

		csvPath := filepath.Join(dataDir, entry.Name())
		tableName := strings.TrimSuffix(entry.Name(), ".csv")

		err = loadCSVFileWithWriter(executor, csvPath, tableName, w, debug)
		if err != nil {
			return fmt.Errorf("failed to load %s: %w", entry.Name(), err)
		}
		csvCount++
	}

	if csvCount == 0 {
		fmt.Fprintln(w, "No CSV files found in data directory")
	} else {
		fmt.Fprintf(w, "Successfully loaded %d CSV files\n", csvCount)
	}

	return nil
}

// loadCSVFileWithWriter reads a CSV file and inserts rows into the specified table
func loadCSVFileWithWriter(executor DatabaseExecutor, csvPath string, tableName string, w io.Writer, debug bool) error {
	fmt.Fprintf(w, "Loading CSV file: %v into table: %v\n", csvPath, tableName)

	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header row to get column names
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV headers: %w", err)
	}

	if len(headers) == 0 {
		return fmt.Errorf("CSV file has no columns")
	}

	// Read all data rows first
	var records [][]string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading CSV: %w", err)
		}

		if len(record) != len(headers) {
			return fmt.Errorf("row has %d columns, expected %d", len(record), len(headers))
		}

		records = append(records, record)
	}

	if len(records) == 0 {
		fmt.Fprintf(w, "No data rows found in %s\n", csvPath)
		return nil
	}

	// Build INSERT statement with VALUES format for all rows
	columnList := strings.Join(headers, ", ")
	var valueGroups []string

	for _, record := range records {
		// Quote string values and escape single quotes
		quotedValues := make([]string, len(record))
		for i, v := range record {
			// Escape single quotes by doubling them
			escaped := strings.ReplaceAll(v, "'", "''")
			quotedValues[i] = fmt.Sprintf("'%s'", escaped)
		}
		valueGroups = append(valueGroups, fmt.Sprintf("(%s)", strings.Join(quotedValues, ", ")))
	}

	// Build complete INSERT statement
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columnList, strings.Join(valueGroups, ", "))

	if debug {
		fmt.Fprintf(w, "  Inserting %d rows into %s\n", len(records), tableName)
	}

	// Execute batch insert
	err = executor.Execute(ctxbg, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to insert rows: %w", err)
	}

	fmt.Fprintf(w, "Successfully loaded %d rows into %s\n", len(records), tableName)
	return nil
}

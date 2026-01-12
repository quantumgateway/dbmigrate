# dbmigrate

Database schema migration tool for ClickHouse with version tracking. Executes SQL scripts against databases using an index.lst file that lists the SQL files to run in order. Tracks applied migrations to prevent duplicate runs and detect schema drift.

## Installation

```sh
go install github.com/qgs/dbmigrate@latest
```

## Supported Database Engines

- **ClickHouse** (`clickhouse`) - Default port: 9000, Default user: default, Default database: insights

## Usage

```
dbmigrate [options]

Options:
    -e          Database engine (only clickhouse supported). Default: clickhouse
    -h          Hostname to connect to. Default: localhost
    -p          Port of the database server (0 = use default: 9000). Default: 0
    -U          Database username. Default: default
    -W          Database password (will prompt if not provided)
    -db         Name of the database. Default: insights
    -path       Path to the root index.lst file. Default: ./index.lst
    -data       Path to directory containing CSV files for test data (optional)
    -version    Show current schema version and exit
    -force      Force re-run migrations even if already applied
    -debug      Enable debug logging. Default: false
```

## Examples

### Initialize ClickHouse Schema

```sh
# Initialize schema (will prompt for password)
dbmigrate -e clickhouse -h localhost -db insights -path ./sql/index.lst

# Initialize with password
dbmigrate -e clickhouse -h localhost -U default -W password -db insights -path ./sql/index.lst

# Initialize with test data
dbmigrate -e clickhouse -h localhost -db insights -path ./sql/index.lst -data ./testdata/csv
```

### Check Schema Version

```sh
dbmigrate -e clickhouse -h localhost -db insights -version
```

Output:
```
Schema version history (most recent first):
--------------------------------------------
Current: 2.0.15 (2024-12-11 23:45:00)
         Compliance frameworks and rules schema
         2.0.14 (2024-12-11 23:44:58) - SCA tables for OAuth, repositories, and scan results
         2.0.13 (2024-12-11 23:44:56) - Chat conversations and messages for AI assistant
         ...
```

### Force Re-run Migrations

```sh
# Skip version checks and re-run all migrations
dbmigrate -e clickhouse -h localhost -db insights -path ./sql/index.lst -force
```

## Version Tracking

### How It Works

1. **Version Header**: Each SQL file should have a version comment at the top:
   ```sql
   -- version: 2.0.5
   -- description: Add JA3 fingerprint database tables

   CREATE TABLE IF NOT EXISTS ja3_fingerprints ...
   ```

2. **schema_versions Table**: Applied migrations are recorded in the `schema_versions` table:
   ```sql
   SELECT version, applied_at, description, filename FROM schema_versions;
   ```

3. **Checksum Validation**: Each file's MD5 checksum is stored. If a file changes after being applied, dbmigrate will detect the mismatch and refuse to run (unless `-force` is used).

4. **Skip Already Applied**: Migrations that have already been applied (same version + checksum) are automatically skipped.

### Version Format

Use semantic versioning: `MAJOR.MINOR.PATCH`

- **MAJOR**: Schema-breaking changes (e.g., dropping tables)
- **MINOR**: New tables/columns (backward compatible)
- **PATCH**: Data fixes, indexes, optimizations

Example versioning for a schema directory:
```
schema_versions.sql  -- version: 2.0.0 (must be first)
schema.sql           -- version: 2.0.1
auth.sql             -- version: 2.0.2
ciphersuites_data.sql -- version: 2.0.3
...
```

## Index File Format

The `index.lst` file lists SQL files to execute in order:

```
# ClickHouse 2.0 Schema
# Schema versions table must be created first
schema_versions.sql
schema.sql
auth.sql
ciphersuites_data.sql
# You can reference other .lst files recursively
migrations/index.lst
```

- **`.sql` files**: Executed as SQL scripts
- **`.lst` files**: Processed recursively to include more files
- **Empty lines and `#` comments**: Ignored

## SQL File Format

Each SQL file should start with version metadata:

```sql
-- version: 2.0.5
-- description: Brief description of what this migration does

-- Your SQL statements here
CREATE TABLE IF NOT EXISTS ...;
ALTER TABLE ...;
INSERT INTO ...;
```

The `-- version:` and `-- description:` comments must be in the first 10 lines of the file.

## Features

- **Version Tracking**: Records applied migrations with timestamps and checksums
- **Idempotent Runs**: Automatically skips already-applied migrations
- **Checksum Validation**: Detects modified migration files
- **Force Mode**: Override version checks when needed
- **CSV Data Loading**: Load test/seed data from CSV files
- **Recursive Processing**: Supports nested .lst files for complex schemas

## Error Handling

### Checksum Mismatch

If a migration file changes after being applied:

```
WARNING: Checksum mismatch for version 2.0.5
  File: sql/ja3_database.sql
  Expected checksum: abc123...
  Current checksum:  def456...
  Use -force to re-apply this migration
```

This usually means someone modified a migration that was already applied. Options:
1. Revert the file changes
2. Create a new migration with a new version number
3. Use `-force` to re-apply (use with caution in production)

### Table Does Not Exist

On first run, the `schema_versions` table won't exist yet. This is normal - the first migration (`schema_versions.sql`) creates it.

## License

MIT

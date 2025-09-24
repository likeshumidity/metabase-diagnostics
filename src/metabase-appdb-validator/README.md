# Metabase Application Database Validator

A comprehensive Babashka CLI tool for validating Metabase application database schemas across all tagged versions.

## Overview

This tool validates Metabase application databases against unified schema definitions extracted from the Metabase codebase. It performs four types of validation:

1. **Structural Validation** - Table/column existence, data types, constraints
2. **Business Rules Validation** - Clojure model validations, foreign key relationships
3. **Data Integrity Validation** - Value constraints, enum validations
4. **Migration State Validation** - Ensuring all migrations have been applied correctly

## Features

- **Multi-Version Schema Support**: Extracts schemas from git tags covering major versions (x.0.0) and latest patch versions
- **PostgreSQL Focus**: Optimized for PostgreSQL with auto-detection and error handling for unsupported databases
- **Unified Schema Definitions**: Combines Clojure model definitions, TypeScript frontend types, Liquibase migrations, and SQL initialization scripts
- **Comprehensive Validation**: Four distinct validation scopes with detailed error reporting
- **CSV Output**: Single Excel-compatible CSV with validation results and metadata
- **Heuristic vs Deterministic**: Clear marking of whether schema definitions are deterministically or heuristically identified

## Schema Source Priority

The tool extracts schema definitions from multiple sources in order of preference:

1. **Clojure Model Definitions** (Deterministic) - Especially for MBQL and query objects
2. **Frontend TypeScript Definitions** (Deterministic) - Especially for visualization settings
3. **Liquibase Migration Files** (Deterministic) - Historical schema evolution
4. **SQL Initialization Scripts** (Deterministic) - Current state snapshots

## Supported Metabase Versions

The tool supports validation against schemas extracted from:

- **Major Version Initials**: v0.10.0, v0.11.0, ..., v1.50.0
- **Latest Patch Versions**: Most recent patch for each major version
- **Current Development**: HEAD/main branch

## Database Support

- **PostgreSQL**: Full support with version detection
- **Other Databases**: Will detect and error with helpful message describing unsupported database type

## Installation

### Prerequisites

- Access to target Metabase application database
- Clojure/Java environment

### Setup

```bash
# Clone and navigate to the validator
cd metabase-diagnostics/src/metabase-appdb-validator

# Make CLI executable
chmod +x metabase-appdb-validator
```

## Usage

### Basic Validation

```bash
# Validate against latest schema
./metabase-appdb-validator validate \
  --db-host localhost \
  --db-port 5432 \
  --db-name metabase \
  --db-user metabase \
  --db-password secretpassword

# Validate against specific Metabase version
./metabase-appdb-validator validate \
  --db-host localhost \
  --db-port 5432 \
  --db-name metabase \
  --db-user metabase \
  --db-password secretpassword \
  --metabase-version v1.50.0
```

### Advanced Options

```bash
# Run specific validation scopes
./metabase-appdb-validator validate \
  --db-host localhost \
  --db-port 5432 \
  --db-name metabase \
  --db-user metabase \
  --db-password secretpassword \
  --scope structural,business-rules

# Output to specific file
./metabase-appdb-validator validate \
  --db-host localhost \
  --db-port 5432 \
  --db-name metabase \
  --db-user metabase \
  --db-password secretpassword \
  --output validation-results.csv

# Verbose mode with detailed logging
./metabase-appdb-validator validate \
  --db-host localhost \
  --db-port 5432 \
  --db-name metabase \
  --db-user metabase \
  --db-password secretpassword \
  --verbose
```

### Schema Extraction

```bash
# Extract schemas for all supported versions
./metabase-appdb-validator extract-schemas

# Extract schema for specific version
./metabase-appdb-validator extract-schemas --version v1.50.0

# List available schema versions
./metabase-appdb-validator list-versions
```

## Configuration

### Environment Variables

```bash
export METABASE_DB_HOST=localhost
export METABASE_DB_PORT=5432
export METABASE_DB_NAME=metabase
export METABASE_DB_USER=metabase
export METABASE_DB_PASSWORD=secretpassword
export METABASE_VERSION=v1.50.0
```

### Config File

Create `config.edn`:

```clojure
{:database {:host "localhost"
            :port 5432
            :name "metabase"
            :user "metabase"
            :password "secretpassword"}
 :metabase-version "v1.50.0"
 :validation-scopes [:structural :business-rules :data-integrity :migration-state]
 :output-file "validation-results.csv"
 :verbose true}
```

## Output Format

The tool outputs a single CSV file with the following columns:

| Column | Description |
|--------|-------------|
| `table_name` | Database table name |
| `column_name` | Column name (empty for table-level validations) |
| `validation_scope` | One of: structural, business-rules, data-integrity, migration-state |
| `check_passed` | Boolean: true/false |
| `error_message` | Detailed error description if check failed |
| `schema_source` | Source of schema definition (clojure-model, typescript-def, liquibase-migration, sql-init) |
| `identification_method` | Either "deterministic" or "heuristic" |
| `metabase_version` | Version schema was extracted from |
| `validation_timestamp` | When validation was performed |

## Validation Scopes

### Structural Validation
- Table existence
- Column existence and data types
- Primary key constraints
- Unique constraints
- Not null constraints
- Foreign key relationships

### Business Rules Validation
- Clojure model validations
- Custom validation functions
- Enum value constraints
- Cross-table relationships
- MBQL query object validation

### Data Integrity Validation
- Value range constraints
- Pattern matching (regex)
- Custom business logic validation
- Referential integrity beyond foreign keys

### Migration State Validation
- All required migrations applied
- Migration checksum validation
- No pending migrations
- Migration order verification

## Schema Definition Architecture

### Clojure Model Extraction
- Parses `defmodel` and `deftable` definitions
- Extracts validation functions and constraints
- Identifies relationships and transformations
- Analyzes MBQL and query object schemas

### TypeScript Definition Extraction
- Parses frontend type definitions
- Extracts visualization settings schemas
- Identifies frontend-backend contract types
- Maps TypeScript interfaces to database schemas

### Liquibase Migration Analysis
- Parses YAML migration files
- Tracks schema evolution across versions
- Identifies incremental changes
- Validates migration dependency chains

### SQL Initialization Script Analysis
- Parses database-specific DDL
- Extracts current state schemas
- Identifies PostgreSQL-specific features
- Maps to canonical schema definitions

## Error Handling

The tool provides detailed error messages for common scenarios:

- **Unsupported Database**: Clear message with supported database list
- **Connection Failures**: Network and authentication troubleshooting
- **Schema Mismatches**: Detailed comparison between expected and actual
- **Version Not Found**: List of available versions
- **Permission Issues**: Database access requirement details

## Development

### Project Structure

```
metabase-appdb-validator/
├── README.md
├── metabase-appdb-validator              # Main CLI script
├── deps.edn                              # Dependencies
├── src/
│   ├── metabase_appdb_validator/
│   │   ├── core.clj                      # Main entry point
│   │   ├── cli.clj                       # CLI argument parsing
│   │   ├── schema/
│   │   │   ├── extractor.clj             # Schema extraction engine
│   │   │   ├── clojure_models.clj        # Clojure model parsing
│   │   │   ├── typescript_defs.clj       # TypeScript definition parsing
│   │   │   ├── liquibase_migrations.clj  # Migration analysis
│   │   │   └── sql_init_scripts.clj      # SQL script parsing
│   │   ├── validation/
│   │   │   ├── structural.clj            # Structural validation
│   │   │   ├── business_rules.clj        # Business rules validation
│   │   │   ├── data_integrity.clj        # Data integrity validation
│   │   │   └── migration_state.clj       # Migration state validation
│   │   ├── database/
│   │   │   ├── connection.clj            # Database connection handling
│   │   │   └── postgresql.clj            # PostgreSQL-specific operations
│   │   └── output/
│   │       └── csv.clj                   # CSV output formatting
└── test/
    └── metabase_appdb_validator/
        ├── schema_test.clj
        ├── validation_test.clj
        └── integration_test.clj
```

### Running Tests

```bash
# Unit tests
bb test

# Integration tests (requires test database)
bb test-integration

# All tests
bb test-all
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Run the test suite
5. Submit a pull request

## Troubleshooting

### Common Issues

**Database Connection Failed**
```
Check database credentials and network connectivity
Verify PostgreSQL is running and accessible
Confirm database name and user permissions
```

**Schema Version Not Found**
```
Run: ./metabase-appdb-validator list-versions
Verify version format (e.g., v1.50.0)
Check if version exists in git tags
```

**Validation Failures**
```
Review error messages in CSV output
Check validation scope selection
Verify database schema matches expected Metabase version
```

### Debug Mode

Enable debug logging:

```bash
export METABASE_APPDB_VALIDATOR_DEBUG=true
./metabase-appdb-validator validate --verbose
```

## License

This project follows the same license as the main Metabase project.

## Support

For issues related to this diagnostic tool:
1. Check the troubleshooting section above
2. Review the CSV output for detailed error messages
3. Run with `--verbose` flag for additional logging
4. Open an issue in the metabase-diagnostics repository

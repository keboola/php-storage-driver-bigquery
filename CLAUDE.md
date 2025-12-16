# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Keboola Storage Driver implementation for Google BigQuery. It provides a high-level backend driver for managing BigQuery projects, datasets (buckets), tables, and workspaces through a command-based architecture using Protocol Buffers.

## Development Commands

### Setup
```bash
# Install dependencies
docker compose run --rm dev composer install

# Load test data to GCS
docker compose run --rm dev composer loadGcs
```

### Testing
```bash
# Run all tests (unit + functional)
docker compose run --rm dev composer tests

# Run all tests in parallel
docker compose run --rm dev composer paratest

# Run specific test suites in parallel
docker compose run --rm dev composer paratest-import    # Import tests only
docker compose run --rm dev composer paratest-export    # Export tests only
docker compose run --rm dev composer paratest-other     # Exclude import/export

# Run unit tests only
docker compose run --rm dev composer tests-unit

# Run functional tests only
docker compose run --rm dev composer tests-functional

# Run a single test file
docker compose run --rm dev vendor/bin/phpunit tests/unit/NameGeneratorTest.php

# Run a specific test method
docker compose run --rm dev vendor/bin/phpunit --filter testMethodName tests/unit/NameGeneratorTest.php
```

### Code Quality
```bash
# Run all quality checks (lint, phpcs, phpstan)
docker compose run --rm dev composer check

# Run full CI workflow (check + unit tests)
docker compose run --rm dev composer ci

# Individual checks
docker compose run --rm dev composer phplint   # PHP linting
docker compose run --rm dev composer phpcs     # Code style check
docker compose run --rm dev composer phpcbf    # Code style auto-fix
docker compose run --rm dev composer phpstan   # Static analysis
```

### Xdebug
Use `dev-xdebug` service instead of `dev` for debugging:
```bash
docker compose run --rm dev-xdebug composer tests
```

### Test Retry Configuration
To disable test retry, copy the retry configuration:
```bash
cp phpunit-retry.xml.dist phpunit-retry.xml
```

## Architecture

### Command-Handler Pattern
The driver uses a command-handler architecture where:
- **Commands** (from `keboola/storage-driver-common`) define operations via Protocol Buffers
- **Handlers** (in `src/Handler/`) implement the operations
- **HandlerFactory** (`src/Handler/HandlerFactory.php`) routes commands to appropriate handlers

Main entry point: `BigQueryDriverClient::runCommand()` receives a command and credentials, creates a handler via HandlerFactory, executes it, and returns a wrapped response.

### Handler Categories
Handlers are organized by domain in `src/Handler/`:
- **Backend/** - Backend initialization and removal
- **Bucket/** - Dataset operations (create, drop, link, share, access control)
- **Table/** - Table operations organized in subdirectories:
  - `Alter/` - Schema changes (add/drop column, primary keys, delete rows)
  - `Create/` - Table creation (including time travel)
  - `Drop/` - Table deletion
  - `Import/` - Import from file or table
  - `Export/` - Export to file
  - `Preview/` - Preview table data
  - `Profile/` - Table profiling/metrics
- **Workspace/** - Workspace operations (create, drop, clear, load, password reset)
- **Project/** - GCP project lifecycle (create, update, drop)
- **ExecuteQuery/** - Raw query execution
- **Info/** - Metadata retrieval

### GCP Client Management
`GCPClientManager` (src/GCPClientManager.php) is the central factory for GCP service clients:
- Creates and configures Google Cloud clients (BigQuery, Storage, IAM, etc.)
- Manages authentication using credentials from `GenericBackendCredentials`
- Configures retry logic, timeouts, and user agent headers
- Used by all handlers to interact with BigQuery and GCP services

### Credentials and Authentication
- `CredentialsHelper` converts `GenericBackendCredentials` to GCP-compatible credential arrays
- Service accounts require `private_key` (stored as `BQ_SECRET`) and other metadata
- See README.md for detailed credential setup using Terraform

### Query Builders
`src/QueryBuilder/` contains SQL generation logic:
- `ExportQueryBuilder` - Generates queries for table exports
- `WorkspaceLoadQueryBuilder` - Generates queries for workspace loading
- `ColumnConverter` - Handles column type conversions
- `CommonFilterQueryBuilder` - Builds filter clauses

### Project ID Format
BigQuery project IDs are composed of `stackPrefix` + `projectId` and must meet these requirements:
- 6-30 characters
- Lowercase letters, numbers, hyphens only
- Start with letter
- Cannot end with hyphen
- Must be globally unique across all GCP

### Import/Export Architecture
- Uses `keboola/db-import-export` library for data transfer operations
- Supports GCS (Google Cloud Storage) as file provider
- Import handlers use `ToStageImporter`, `FullImporter`, `IncrementalImporter` from the library
- File formats: CSV with configurable delimiters, enclosures, escape characters

### Testing Structure
- **Unit tests** (`tests/unit/`) - Fast, isolated tests for utilities and builders
- **Functional tests** (`tests/functional/`) - Integration tests against real BigQuery
  - `BaseCase.php` - Base test class with project setup/teardown
  - `UseCase/` - Organized by feature (Table/Import, Table/Export, etc.)
  - Requires `.env` file with BigQuery credentials (see README.md)
  - Uses parallel execution via paratest for faster CI runs
- **Test data** - `tests/data/` contains CSV files and sample data
- **Stub loader** - `tests/StubLoader/` loads test data to GCS

### Environment Configuration
Required environment variables (in `.env`):
- `BQ_PRINCIPAL` - Service account JSON (without private_key)
- `BQ_SECRET` - Private key from service account
- `BQ_FOLDER_ID` - GCP folder ID for organizing projects
- `BQ_STACK_PREFIX` - Prefix for project naming (use different than Terraform to avoid conflicts)
- `BQ_BUCKET_NAME` - GCS bucket for file storage
- `BQ_KEY_FILE` - Service account JSON for main service account

## Working with Dependencies

### Local Development with keboola/storage-driver-common or keboola/table-backend-utils
When making changes to shared packages:
1. Remove vendor folders: `rm -r ./vendor/keboola/storage-driver-common ./vendor/keboola/table-backend-utils`
2. Symlink local packages: `ln -s /path/to/local/package ./vendor/keboola/...`
3. Create `docker-compose.override.yml` to mount local paths into container (see README.md example)

## Common Patterns

### Handler Implementation
All handlers extend `BaseHandler` and implement:
- Constructor accepting `GCPClientManager`
- `__invoke()` method with signature: `(GenericBackendCredentials, Message, array, Message): ?Message`
- Use `$this->clientManager` to get GCP clients
- Return appropriate response message or null
- Log messages via `$this->addLogMessage()` inherited from BaseHandler

### BigQuery Client Wrapper
Most handlers use `BigqueryTableQueryBuilder` and `BigqueryTableReflection` from `keboola/table-backend-utils`:
- `BigqueryTableQueryBuilder` - Fluent API for DDL/DML
- `BigqueryTableReflection` - Introspect table metadata
- `BigqueryTableDefinition` - Define table schema

### Error Handling
- Use domain-specific exceptions from `keboola/storage-driver-common`
- `ExceptionHandler` provides utilities for GCP error translation
- Handlers should catch GCP exceptions and rethrow with context

## BigQuery-Specific Considerations

### Naming Conventions
- `NameGenerator` provides utilities for generating compliant GCP resource names
- Dataset names (buckets) must be alphanumeric + underscores
- Table names follow BigQuery identifier rules
- Project IDs have strict format requirements (see above)

### Locations and Regions
- Datasets have location constraints (EU, US, etc.)
- Default location: US
- Location must match between related resources
- Some operations require specific regional configurations

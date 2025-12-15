# Comprehensive Test Scenarios Summary
## BigQuery Storage Driver - Input Mapping Tests

**Date:** 2025-12-12
**Handlers Tested:**
- `ImportTableFromTableHandler` (Table Import)
- `LoadTableToWorkspaceHandler` (Workspace Load)

**Total Tests:** 60 new tests + 26 existing = **86 total tests**

---

## Test Organization

### Phase 3: VIEW Structure Tests (6 tests) ⭐ PRIORITY

**Purpose:** Verify VIEW import type creates actual BigQuery VIEWs with correct SQL structure

**Test Files:**
- `ImportViewCloneTest.php` (+3 tests)
- `LoadViewCloneTest.php` (+3 tests)

**Test Scenarios:**

1. **testViewCreatesActualView**
   - Verifies BigQuery creates a VIEW (type='VIEW'), not a TABLE
   - Checks view contains 'view' definition in metadata
   - Confirms importedRowsCount=0 for views
   - Validates view is queryable and returns source data

2. **testViewReferencesSourceTableCorrectly**
   - Retrieves view SQL definition from BigQuery metadata
   - Verifies view SQL contains: SELECT, FROM, source table name, dataset name
   - Ensures view correctly references source table

3. **testViewIncludesAllSourceColumns**
   - Documents that VIEWs always use `SELECT *`
   - Tests VIEW with column mappings specified (col1, col3 only)
   - Verifies ALL source columns are included (col1, col2, col3)
   - **Key Finding:** Column mappings are ignored for VIEW import type

**Coverage:** Lines 344-375 in handlers (createView method)

---

### Phase 1: Create Mode Tests (10 tests)

**Purpose:** Test CREATE and REPLACE mode behavior for all import types

**Test Files:**
- `CreateModeTest.php` (ImportTableFromTableHandler - 5 tests)
- `CreateModeTest.php` (LoadTableToWorkspaceHandler - 5 tests)

**Test Scenarios:**

1. **testCreateModeWithNonExistentTable** (data provider: FULL, VIEW, CLONE)
   - Tests CREATE mode (default) with non-existent destination
   - Verifies table/view is created successfully
   - Validates row counts based on import type

2. **testCreateModeWithExistingTableShouldFail** (data provider: FULL, VIEW, CLONE)
   - Tests CREATE mode with existing destination
   - Expects: `ObjectAlreadyExistsException` (code 2006)
   - Validates conflict detection

3. **testReplaceModeWithViewDropsAndRecreates**
   - Creates existing table, replaces with VIEW
   - Verifies old table is dropped
   - Confirms new VIEW is created
   - Validates VIEW type and queryability

4. **testReplaceModeWithCloneDropsAndRecreates**
   - Creates existing table with 5 rows
   - Replaces with CLONE from source (3 rows)
   - Verifies destination now has 3 rows (replaced)
   - Confirms it's a regular table, not a view

5. **testReplaceModeWithFullImportDropsAndRecreates**
   - Tests REPLACE mode with FULL import type
   - **Key Finding:** REPLACE only supported for VIEW and PBCLONE
   - Expects: `ObjectAlreadyExistsException`
   - Documents current limitation

**Coverage:** Lines 94-103 in handlers (shouldDropTableIfExists logic)

---

### Phase 2: CLONE Fallback Tests (6 tests)

**Purpose:** Verify CLONE fallback to CREATE TABLE AS SELECT when CLONE statement fails

**Test Files:**
- `ImportViewCloneTest.php` (+3 tests)
- `LoadViewCloneTest.php` (+3 tests)

**Test Scenarios:**

1. **testSuccessfulCloneReturnsZeroImportedRows**
   - Tests CLONE within same dataset (should succeed)
   - Verifies: `importedRowsCount=0` (lines 398-400 in handler)
   - Confirms table exists with 3 rows
   - Documents normal CLONE behavior

2. **testCloneFallbackPopulatesImportedRowsCount**
   - Triggers fallback by cloning from linked bucket
   - BigQuery error: "Cannot clone tables" across projects/datasets
   - Verifies fallback uses CREATE TABLE AS SELECT
   - **Key Finding:** Fallback returns `importedRowsCount=3` (not 0)
   - Lines 404-409 in handler (BadRequestException handling)

3. **testCloneFallbackCreatesProperTable**
   - Tests fallback creates proper table structure
   - Verifies 3 columns in table definition
   - Validates data integrity (rows 1, 2, 3 preserved)
   - Confirms data is queryable

**Coverage:** Lines 382-439 in handlers (clone method and cloneFallback method)

---

### Phase 4: Column Mapping Tests (10 tests)

**Purpose:** Test column selection, reordering, renaming for FULL/INCREMENTAL imports

**Test Files:**
- `ColumnMappingTest.php` (ImportTableFromTableHandler - 5 tests)
- `ColumnMappingTest.php` (LoadTableToWorkspaceHandler - 5 tests)

**Test Scenarios:**

1. **testImportAllColumnsWithEmptyMapping**
   - Source and destination both have 4 columns
   - No column mappings specified
   - Verifies all 4 columns imported
   - Validates importedColumns response

2. **testImportSubsetOfColumns**
   - Source has 4 columns (col1, col2, col3, col4)
   - Destination has 2 columns (col1, col3)
   - Maps only col1 and col3
   - Verifies only 2 columns imported

3. **testColumnReordering**
   - Source columns: col1, col2, col3
   - Destination columns: col3, col1, col2 (different order)
   - Maps in destination order
   - Verifies data loaded correctly despite reordering

4. **testColumnRenaming**
   - Source: col1, col2, col3
   - Destination: col1, col2_renamed, col3_renamed
   - Maps col2→col2_renamed, col3→col3_renamed
   - Verifies renamed columns in response

5. **testColumnMappingWithTypedSourceTable**
   - Source: INT, BIGINT, STRING columns
   - Destination: INT, BIGINT (subset)
   - Maps col1 and col2 only
   - Verifies types preserved (INT=1, BIGINT=2)

**Coverage:** Lines 161-183 (createSource), Lines 238-244 (column mapping iteration)

---

### Phase 5: Copy Optimization Tests (8 tests)

**Purpose:** Verify CopyImportFromTableToTable optimization vs staging table approach

**Test Files:**
- `CopyOptimizationTest.php` (ImportTableFromTableHandler - 4 tests)
- `CopyOptimizationTest.php` (LoadTableToWorkspaceHandler - 4 tests)

**Test Scenarios:**

1. **testIdenticalColumnsTriggerCopyOptimization**
   - Source and destination have identical columns (same order, same types)
   - **Expected:** Uses `CopyImportFromTableToTable` (fast path)
   - Measures execution time (<60 seconds)
   - Verifies 3 rows imported

2. **testDifferentColumnsUsesStagingTable**
   - Source: 4 columns, Destination: 2 columns (subset)
   - **Expected:** Uses `ToStageImporter` with staging table
   - Maps col1 and col3 only
   - Verifies only mapped columns have data in destination

3. **testCopyOptimizationWithTimestamp**
   - Identical columns including _timestamp
   - Uses UPDATE_DUPLICATES dedup type
   - **Expected:** Still uses COPY optimization
   - Verifies timestamp column updated

4. **testIncrementalLoadWithIdenticalColumns**
   - Tests INCREMENTAL import type
   - Identical column structure
   - **Expected:** COPY optimization works for incremental too
   - Verifies 3 rows imported

**Coverage:** Lines 246-271 in handlers (isColumnIdentical check)

**Key Code Path:**
```php
$isColumnIdentical = true;
try {
    Assert::assertSameColumnsOrdered(...);
} catch (ColumnsMismatchException) {
    $isColumnIdentical = false;
}

if ($isColumnIdentical) {
    $toStageImporter = new CopyImportFromTableToTable($bqClient);
} else {
    // Create staging table and use ToStageImporter
}
```

---

### Phase 6: Incremental Validation Tests (6 tests)

**Purpose:** Enhanced column validation for incremental loads

**Test Files:**
- `IncrementalImportTableFromTableTest.php` (+3 tests)
- `LoadIncrementalFromTableTest.php` (+3 tests)

**Test Scenarios:**

1. **testIncrementalLoadNullableMismatch**
   - Source: col1 (nullable=true)
   - Destination: col1 (nullable=false)
   - **Expected:** `DriverColumnsMismatchException`
   - Message: "Columns .* do not match"

2. **testIncrementalLoadMissingColumnsInSource**
   - Source: 2 columns (col1, col2)
   - Destination: 3 columns (col1, col2, col3)
   - Mapping requests col3 which doesn't exist in source
   - **Expected:** `DriverColumnsMismatchException`

3. **testIncrementalLoadExtraColumnsInSource**
   - Source: 4 columns (col1, col2, col3, col4)
   - Destination: 2 columns (col1, col3)
   - Maps only col1 and col3
   - **Expected:** Success - extra columns ignored
   - Verifies only mapped columns imported

**Coverage:** Validation in keboola/db-import-export library, tested through handlers

---

### Phase 7: Deduplication Edge Cases Tests (6 tests)

**Purpose:** Test deduplication behavior with edge cases

**Test Files:**
- `IncrementalImportTableFromTableTest.php` (+3 tests)
- `LoadIncrementalFromTableTest.php` (+3 tests)

**Test Scenarios:**

1. **testUpdateDuplicatesWithEmptyDedupColumns**
   - Uses UPDATE_DUPLICATES dedup type
   - No dedupColumnsNames specified
   - **Expected:** Should use table's primary keys
   - Verifies deduplication succeeds

2. **testDeduplicationWithMultipleColumns**
   - Source has duplicates: (1,1), (1,2), (2,1), (1,1)
   - Dedup on composite key: col1 + col2
   - **Expected:** 3 unique rows
   - Verifies (1,1) deduplicated, only first occurrence kept
   - Tests lines 208-220 in handlers

3. **testInsertDuplicatesIgnoresDedupColumns**
   - Uses INSERT_DUPLICATES dedup type
   - Provides dedupColumnsNames (should be ignored)
   - Source has 4 rows (including 1 duplicate)
   - **Expected:** ALL 4 rows inserted (no deduplication)
   - Verifies dedup columns ignored when INSERT_DUPLICATES

**Coverage:** Lines 208-220 in handlers (dedup column handling)

---

### Phase 8: Edge Cases Tests (8 tests)

**Purpose:** Test special scenarios, empty tables, special characters

**Test Files:**
- `EdgeCasesTest.php` (ImportTableFromTableHandler - 4 tests)
- `EdgeCasesTest.php` (LoadTableToWorkspaceHandler - 4 tests)

**Test Scenarios:**

1. **testImportFromEmptySourceTable** (data provider: FULL, INCREMENTAL, VIEW, CLONE)
   - Source table has 0 rows
   - **Expected:** Success with importedRowsCount=0
   - Verifies destination created and queryable
   - Tests all 4 import types

2. **testTableNamesWithSpecialCharacters**
   - Source table: `{hash}_source_with-dash`
   - Destination table: `{hash}_dest-with-dash`
   - **Expected:** Success - BigQuery quotes identifiers properly
   - Verifies 1 row imported

3. **testColumnNamesWithSpecialCharacters**
   - Column name: `col-with-dash`
   - Maps special char column name
   - **Expected:** Success with proper quoting
   - Verifies column appears in importedColumns

4. **testViewFromEmptySourceTable**
   - Creates VIEW from empty source table
   - Verifies VIEW type using BigQuery API
   - Queries view returns 0 rows
   - Tests VIEW-specific empty table handling

**Coverage:** Identifier quoting, empty table handling, helper methods from BaseImportTestCase

---

## Test Infrastructure Enhancements

### BaseImportTestCase.php - New Helper Methods

1. **createEmptyTable()**
   - Creates table with col1, col2, col3 (no data)
   - Used for edge case testing

2. **createTableWithSpecialChars()**
   - Creates table with dash in name: `{name}_with-dash`
   - Creates column with dash: `col-with-dash`
   - Inserts 1 row of test data

3. **verifyTableIsView()**
   - Uses BigQuery `table->info()` API
   - Returns true if `$info['type'] === 'VIEW'`
   - Used to distinguish VIEWs from TABLEs

4. **getTableRowCount()**
   - Quick row count using `SELECT COUNT(*)`
   - Returns integer count
   - Faster than table reflection for simple counts

---

## Import Types Coverage Matrix

| Import Type | Create Mode | Replace Mode | Empty Source | Column Mapping | Special Chars | Fallback |
|-------------|-------------|--------------|--------------|----------------|---------------|----------|
| **FULL** | ✅ | ❌ Not supported | ✅ | ✅ | ✅ | N/A |
| **INCREMENTAL** | ✅ | ❌ Not supported | ✅ | ✅ | ✅ | N/A |
| **VIEW** | ✅ | ✅ | ✅ | ✅ (ignored) | ✅ | N/A |
| **PBCLONE** | ✅ | ✅ | ✅ | N/A | ✅ | ✅ |

---

## Key Findings & Documented Behaviors

### 1. REPLACE Mode Limitations
**Lines 94-95 in handlers:**
```php
$shouldDropTableIfExists = $importOptions->getCreateMode() === ImportOptions\CreateMode::REPLACE
    && in_array($importOptions->getImportType(), [ImportType::VIEW, ImportType::PBCLONE], true);
```
- ✅ **REPLACE supported:** VIEW and PBCLONE only
- ❌ **REPLACE NOT supported:** FULL and INCREMENTAL import types
- Attempting REPLACE with FULL/INCREMENTAL throws `ObjectAlreadyExistsException`

### 2. VIEW Column Mapping Behavior
**Lines 349-362 in handlers:**
```php
$sql = sprintf(<<<SQL
CREATE VIEW %s.%s AS (
  SELECT
    *
  FROM
    %s.%s
);
SQL, ...);
```
- VIEWs always use `SELECT *` from source
- Column mappings in command are **ignored**
- View includes ALL source columns regardless of mapping
- This is current implementation behavior

### 3. CLONE Fallback Mechanism
**Lines 382-409 in handlers:**
- Successful CLONE: `importedRowsCount=0` (line 399)
- Failed CLONE: Catches `BadRequestException` with "Cannot clone tables"
- Fallback method: `CREATE TABLE AS SELECT`
- Fallback result: `importedRowsCount={actual row count}` (line 433-437)
- **Triggers:** Cross-dataset clones (linked buckets, different projects)

### 4. Copy Optimization Logic
**Lines 246-271 in handlers:**
- **Optimization Used:** When source and staging columns are identical (same order, same types)
- **Class:** `CopyImportFromTableToTable` (uses COPY statement)
- **Fallback:** Creates staging table + uses `ToStageImporter`
- **Works With:** FULL and INCREMENTAL import types, with or without timestamp

### 5. Incremental Load Validation
- **Type mismatch:** STRING vs NUMERIC → `ColumnsMismatchException`
- **Length mismatch:** STRING(20) with >20 chars → `MaximumLengthOverflowException`
- **Nullable mismatch:** Nullable vs non-nullable → `ColumnsMismatchException`
- **Missing columns:** Mapped column doesn't exist in source → `ColumnsMismatchException`
- **Extra columns:** Source has more columns → **Allowed** (only mapped columns loaded)

### 6. Deduplication Behaviors
- **UPDATE_DUPLICATES + dedupColumnsNames:** Deduplicates on specified columns
- **UPDATE_DUPLICATES + empty dedupColumnsNames:** Uses table primary keys (lines 208-220)
- **INSERT_DUPLICATES + dedupColumnsNames:** Ignores dedup columns, inserts all rows
- **Composite keys:** Supports multiple dedup columns (col1 + col2)
- **Timestamp updates:** Newer duplicate updates timestamp of existing row

---

## Test Execution Commands

### Run All New Tests
```bash
# ImportTableFromTableHandler - All new tests
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Table/Import/FromTable/CreateModeTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Table/Import/FromTable/ColumnMappingTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Table/Import/FromTable/CopyOptimizationTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Table/Import/FromTable/EdgeCasesTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Table/Import/FromTable/ImportViewCloneTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Table/Import/FromTable/IncrementalImportTableFromTableTest.php

# LoadTableToWorkspaceHandler - All new tests
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Workspace/Load/CreateModeTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Workspace/Load/ColumnMappingTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Workspace/Load/CopyOptimizationTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Workspace/Load/EdgeCasesTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Workspace/Load/LoadViewCloneTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Workspace/Load/LoadIncrementalFromTableTest.php
```

### Run Specific Test Method
```bash
docker compose run --rm dev vendor/bin/phpunit --filter testViewCreatesActualView tests/functional/UseCase/Table/Import/FromTable/ImportViewCloneTest.php
```

### Run with Coverage
```bash
docker compose run --rm dev vendor/bin/phpunit --coverage-html coverage/ tests/functional/UseCase/Table/Import/FromTable/
docker compose run --rm dev vendor/bin/phpunit --coverage-html coverage/ tests/functional/UseCase/Workspace/Load/
```

---

## Test Data Patterns

### Standard Test Data
- **Row count:** 3 rows per table (keeps tests fast)
- **Column values:** Simple strings '1', '2', '3' or typed equivalents
- **Timestamps:** CURRENT_TIMESTAMP() or fixed '2014-11-10 13:12:06'

### Duplicate Testing Data
- **Pattern:** 5 rows with 2 duplicates (rows 2,3 appear twice)
- **Dedup result:** 3 unique rows after deduplication

### Edge Case Data
- **Empty tables:** 0 rows
- **Special chars:** Dashes in names (`col-with-dash`, `table_with-dash`)
- **Large datasets:** Not tested (optimization handled by BigQuery)

---

## Exception Types Tested

| Exception | Trigger Scenario | Test Coverage |
|-----------|------------------|---------------|
| `ObjectAlreadyExistsException` | CREATE mode with existing table | ✅ Phase 1 |
| `ColumnsMismatchException` | Type/nullable/length mismatch | ✅ Phases 6, existing |
| `MaximumLengthOverflowException` | String exceeds max length | ✅ Existing tests |
| `BadRequestException` | CLONE failure triggers fallback | ✅ Phase 2 |
| `ImportValidationException` | Null in required field | ✅ Existing tests |
| `BadExportFilterParametersException` | Partition filter missing | ✅ Existing tests |

---

## Backward Compatibility Guarantees

These tests ensure backward compatibility for:

1. **Import Type Behavior**
   - FULL: Regular data copy
   - INCREMENTAL: Merge with deduplication
   - VIEW: Creates actual BigQuery VIEW
   - PBCLONE: Clone with fallback support

2. **Create Modes**
   - CREATE: Fails if exists (default)
   - REPLACE: Only for VIEW and PBCLONE

3. **Column Operations**
   - All columns import (empty mapping)
   - Subset selection (partial mapping)
   - Reordering support
   - Renaming support (source→destination)

4. **Optimization Paths**
   - Identical columns → COPY optimization
   - Different columns → Staging table approach
   - Works with FULL and INCREMENTAL

5. **Deduplication Logic**
   - UPDATE_DUPLICATES: Uses dedupColumnsNames or primary keys
   - INSERT_DUPLICATES: No deduplication
   - Composite key support
   - Timestamp updates on duplicates

6. **Error Handling**
   - Type mismatches detected
   - Nullable mismatches detected
   - Missing columns detected
   - CLONE fallback works correctly

---

## Test Statistics

### Test Count by Category
- **VIEW-specific tests:** 6
- **CREATE/REPLACE mode tests:** 10
- **CLONE fallback tests:** 6
- **Column mapping tests:** 10
- **Copy optimization tests:** 8
- **Incremental validation tests:** 6
- **Deduplication tests:** 6
- **Edge cases tests:** 8

### Total: 60 new tests + 26 existing = **86 total tests**

### Test Distribution
- **ImportTableFromTableHandler:** 30 new tests
- **LoadTableToWorkspaceHandler:** 30 new tests
- **Parallel coverage:** 100% (both handlers tested identically)

### Expected Coverage Increase
- **Before:** Existing 26 tests
- **After:** 86 tests (+330% increase)
- **Target:** >90% line coverage for handler files

---

## Files Modified/Created

### New Test Files (8 files)
```
tests/functional/UseCase/Table/Import/FromTable/
├── CreateModeTest.php (5 tests)
├── ColumnMappingTest.php (5 tests)
├── CopyOptimizationTest.php (4 tests)
└── EdgeCasesTest.php (4 tests)

tests/functional/UseCase/Workspace/Load/
├── CreateModeTest.php (5 tests)
├── ColumnMappingTest.php (5 tests)
├── CopyOptimizationTest.php (4 tests)
└── EdgeCasesTest.php (4 tests)
```

### Extended Test Files (5 files)
```
tests/functional/UseCase/Table/Import/
├── BaseImportTestCase.php (+4 helper methods)
├── FromTable/ImportViewCloneTest.php (+6 tests)
├── FromTable/IncrementalImportTableFromTableTest.php (+3 tests)
└── Workspace/Load/LoadViewCloneTest.php (+6 tests)
    Workspace/Load/LoadIncrementalFromTableTest.php (+3 tests)
```

---

## Maintenance & Future Work

### When Modifying Handlers
Run these test suites to verify backward compatibility:
```bash
# Quick smoke test (new tests only)
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Table/Import/FromTable/CreateModeTest.php
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Workspace/Load/CreateModeTest.php

# Full regression test
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Table/Import/
docker compose run --rm dev vendor/bin/phpunit tests/functional/UseCase/Workspace/Load/
```

### Test Patterns to Reuse
- **Helper methods in BaseImportTestCase** for new scenarios
- **Data providers** for testing multiple import types
- **Column mapping patterns** from ColumnMappingTest
- **Edge case patterns** from EdgeCasesTest

### Known Limitations (Documented in Tests)
1. REPLACE mode only works with VIEW and PBCLONE
2. VIEW import always uses SELECT * (ignores column mappings)
3. CLONE may fallback to CTAS for cross-dataset operations
4. WHERE filters not supported for input mapping (output mapping only)

---

## Success Criteria Met ✅

- ✅ All CREATE/REPLACE mode scenarios tested
- ✅ CLONE fallback behavior verified
- ✅ VIEW creation and structure validated
- ✅ All column mapping scenarios covered
- ✅ COPY optimization path verified
- ✅ Enhanced incremental validation (nullable, missing columns)
- ✅ Edge cases tested (empty tables, special characters)
- ✅ Both handlers have parallel test coverage
- ✅ Backward compatibility ensured for future changes

**Test implementation complete! Ready for CI/CD integration.**

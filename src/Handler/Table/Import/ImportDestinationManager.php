<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Datatype\Definition\Bigquery as BigqueryDefinition;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ColumnsMismatchException as DriverColumnsMismatchException;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table as CommandDestination;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;

/**
 * Manages destination table operations for imports.
 *
 * This class handles:
 * - Resolving existing destination tables
 * - Creating new destination tables when needed
 * - Validating destination tables for incremental imports
 * - Ensuring column compatibility between source and destination
 */
final class ImportDestinationManager
{
    private BigQueryClient $bqClient;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
    }

    /**
     * Resolves destination table definition, creating it if necessary.
     *
     * For VIEW and PBCLONE imports, the table/view will be created by the respective
     * import method, so this won't create it to avoid conflicts.
     *
     * @param CommandDestination $destination Destination table configuration
     * @param ImportOptions $importOptions Import operation options
     * @param ColumnCollection $expectedColumns Expected column structure
     * @return BigqueryTableDefinition The resolved or created destination definition
     * @throws TableNotExistsReflectionException When table doesn't exist and can't be created
     */
    public function resolveDestination(
        CommandDestination $destination,
        ImportOptions $importOptions,
        ColumnCollection $expectedColumns,
    ): BigqueryTableDefinition {
        $path = ProtobufHelper::repeatedStringToArray($destination->getPath());
        assert(isset($path[0]), 'TableImportFromTableCommand.destination.path is required.');

        $schemaName = $path[0];
        $tableName = $destination->getTableName();

        $reflection = new BigqueryTableReflection($this->bqClient, $schemaName, $tableName);

        try {
            $tableDefinition = $reflection->getTableDefinition();
            assert($tableDefinition instanceof BigqueryTableDefinition);
            return $tableDefinition;
        } catch (TableNotExistsReflectionException $e) {
            return $this->handleNonExistentDestination(
                $schemaName,
                $tableName,
                $importOptions,
                $expectedColumns,
            );
        }
    }

    /**
     * Validates destination table for incremental imports with UPDATE_DUPLICATES.
     *
     * Ensures:
     * - All destination columns (except system columns) exist in source
     * - All expected columns exist in destination
     * - Column definitions match (type and length)
     *
     * Note: Nullability differences are allowed because:
     * 1. BigQuery VIEWs don't preserve NOT NULL constraints
     * 2. BigQuery enforces NOT NULL at INSERT time, not schema validation
     * 3. This enables valid flows like: Table(NOT NULL) → VIEW → Table → Table(NOT NULL)
     *
     * @param BigqueryTableDefinition $destinationDefinition Actual destination table definition
     * @param ColumnCollection $expectedColumns Expected columns from source
     * @param BigqueryTableDefinition $sourceDefinition Source table definition for error messages
     * @throws DriverColumnsMismatchException When validation fails
     */
    public function validateIncrementalDestination(
        BigqueryTableDefinition $destinationDefinition,
        ColumnCollection $expectedColumns,
        BigqueryTableDefinition $sourceDefinition,
    ): void {
        $actualColumns = $this->normalizeColumnsByName(
            $destinationDefinition->getColumnsDefinitions(),
        );
        $expectedColumnsNormalized = $this->normalizeColumnsByName($expectedColumns);

        // Exclude system columns like _timestamp from validation as they're auto-managed
        $systemColumns = ['_timestamp'];
        $actualColumnsFiltered = array_filter(
            $actualColumns,
            fn($key) => !in_array($key, $systemColumns, true),
            ARRAY_FILTER_USE_KEY,
        );

        $this->validateNoMissingSourceColumns(
            $actualColumnsFiltered,
            $expectedColumnsNormalized,
            $sourceDefinition,
        );

        $this->validateNoMissingDestinationColumns(
            $expectedColumnsNormalized,
            $actualColumns,
            $destinationDefinition,
        );

        $this->validateColumnDefinitions(
            $expectedColumnsNormalized,
            $actualColumns,
        );
    }

    /**
     * Creates a new destination table with specified columns and primary keys.
     *
     * @param string $schemaName Schema (dataset) name
     * @param string $tableName Table name
     * @param ColumnCollection $columns Column definitions
     * @param string[] $primaryKeys Primary key column names
     */
    public function createTable(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys,
    ): void {
        $sql = (new BigqueryTableQueryBuilder())->getCreateTableCommand(
            $schemaName,
            $tableName,
            $columns,
            $primaryKeys,
        );

        $this->bqClient->runQuery($this->bqClient->query($sql));
    }

    /**
     * Handles case when destination table doesn't exist.
     *
     * Creates the table unless it's a VIEW or PBCLONE import
     * (those will be created by their respective methods).
     *
     * @param string $schemaName Schema name
     * @param string $tableName Table name
     * @param ImportOptions $importOptions Import options
     * @param ColumnCollection $expectedColumns Expected columns
     * @return BigqueryTableDefinition The new table definition
     */
    private function handleNonExistentDestination(
        string $schemaName,
        string $tableName,
        ImportOptions $importOptions,
        ColumnCollection $expectedColumns,
    ): BigqueryTableDefinition {
        // For VIEW and CLONE imports, table will be created by respective method
        // Don't create it here to avoid conflicts
        $isViewOrClone = in_array(
            $importOptions->getImportType(),
            [ImportType::VIEW, ImportType::PBCLONE],
            true,
        );

        if (!$isViewOrClone) {
            $this->createTable(
                $schemaName,
                $tableName,
                $expectedColumns,
                ProtobufHelper::repeatedStringToArray(
                    $importOptions->getDedupColumnsNames(),
                ),
            );
        }

        return new BigqueryTableDefinition(
            $schemaName,
            $tableName,
            false,
            $expectedColumns,
            ProtobufHelper::repeatedStringToArray(
                $importOptions->getDedupColumnsNames(),
            ),
        );
    }

    /**
     * Validates that no columns are missing in the source table.
     *
     * @param array<string, BigqueryColumn> $actualColumnsFiltered Destination columns (without system columns)
     * @param array<string, BigqueryColumn> $expectedColumnsNormalized Expected source columns
     * @param BigqueryTableDefinition $sourceDefinition Source definition for error message
     * @throws DriverColumnsMismatchException When columns are missing in source
     */
    private function validateNoMissingSourceColumns(
        array $actualColumnsFiltered,
        array $expectedColumnsNormalized,
        BigqueryTableDefinition $sourceDefinition,
    ): void {
        $missingInSource = array_values(
            array_diff(
                array_keys($actualColumnsFiltered),
                array_keys($expectedColumnsNormalized),
            ),
        );

        if ($missingInSource !== []) {
            $missingColumnNames = array_map(
                fn(string $key) => $actualColumnsFiltered[$key]->getColumnName(),
                $missingInSource,
            );

            throw new DriverColumnsMismatchException(sprintf(
                'Some columns are missing in source table "%s"."%s". Missing columns: "%s"',
                $sourceDefinition->getSchemaName(),
                $sourceDefinition->getTableName(),
                implode(',', $missingColumnNames),
            ));
        }
    }

    /**
     * Validates that no columns are missing in the destination table.
     *
     * @param array<string, BigqueryColumn> $expectedColumnsNormalized Expected columns
     * @param array<string, BigqueryColumn> $actualColumns Actual destination columns
     * @param BigqueryTableDefinition $destinationDefinition Destination definition for error message
     * @throws DriverColumnsMismatchException When columns are missing in destination
     */
    private function validateNoMissingDestinationColumns(
        array $expectedColumnsNormalized,
        array $actualColumns,
        BigqueryTableDefinition $destinationDefinition,
    ): void {
        $missingInWorkspace = array_values(
            array_diff(
                array_keys($expectedColumnsNormalized),
                array_keys($actualColumns),
            ),
        );

        if ($missingInWorkspace !== []) {
            $missingColumnNames = array_map(
                fn(string $key) => $expectedColumnsNormalized[$key]->getColumnName(),
                $missingInWorkspace,
            );

            throw new DriverColumnsMismatchException(sprintf(
                'Some columns are missing in workspace table "%s"."%s". Missing columns: "%s"',
                $destinationDefinition->getSchemaName(),
                $destinationDefinition->getTableName(),
                implode(',', $missingColumnNames),
            ));
        }
    }

    /**
     * Validates that column definitions match between expected and actual.
     *
     * @param array<string, BigqueryColumn> $expectedColumnsNormalized Expected column definitions
     * @param array<string, BigqueryColumn> $actualColumns Actual column definitions
     * @throws DriverColumnsMismatchException When column definitions don't match
     */
    private function validateColumnDefinitions(
        array $expectedColumnsNormalized,
        array $actualColumns,
    ): void {
        $definitionErrors = [];

        foreach ($expectedColumnsNormalized as $name => $expectedColumn) {
            $actualColumn = $actualColumns[$name];

            if (!$this->columnDefinitionsMatch($expectedColumn, $actualColumn)) {
                $definitionErrors[] = $this->buildDefinitionErrorMessage(
                    $expectedColumn,
                    $actualColumn,
                );
            }
        }

        if ($definitionErrors !== []) {
            throw new DriverColumnsMismatchException(sprintf(
                'Column definitions mismatch. Details: %s',
                implode('; ', $definitionErrors),
            ));
        }
    }

    /**
     * Checks if two column definitions match.
     *
     * For workspace string tables, allows any type -> STRING conversion.
     * For typed tables, requires exact type and length match.
     *
     * Note: Nullability differences are allowed to support BigQuery VIEW limitations.
     * See inline comments in method body for detailed explanation.
     *
     * @param BigqueryColumn $expected Expected column definition
     * @param BigqueryColumn $actual Actual column definition
     * @return bool True if definitions match
     */
    private function columnDefinitionsMatch(
        BigqueryColumn $expected,
        BigqueryColumn $actual,
    ): bool {
        /** @var BigqueryDefinition $expectedDef */
        $expectedDef = $expected->getColumnDefinition();
        /** @var BigqueryDefinition $actualDef */
        $actualDef = $actual->getColumnDefinition();

        // For workspace string tables, destination columns are always STRING type
        // regardless of source type, so allow any type -> STRING conversion
        // Also be lenient with nullability and length for string tables
        if (strcasecmp($actualDef->getType(), BigqueryDefinition::TYPE_STRING) === 0) {
            return true;
        }

        // For typed tables, require type match (with normalization for aliases)
        // Normalize types to handle BigQuery type aliases (INT/INT64/INTEGER/BIGINT all represent integers)
        $expectedType = self::normalizeType($expectedDef->getType());
        $actualType = self::normalizeType($actualDef->getType());

        if (strcasecmp($expectedType, $actualType) !== 0) {
            return false;
        }

        /*
         * NULLABILITY VALIDATION
         * ----------------------
         * We allow mismatches in nullability for the following reasons:
         *
         * 1. BigQuery VIEWs don't preserve NOT NULL constraints - all columns become nullable
         *    This means a flow like: Table(NOT NULL) → VIEW(nullable) → Table(nullable) → Table(NOT NULL)
         *    would fail validation even though it's a valid use case
         *
         * 2. BigQuery enforces NOT NULL at INSERT/UPDATE time, not at schema level
         *    - Source nullable → Dest NOT NULL: Safe, will fail on actual NULL values during insert
         *    - Source NOT NULL → Dest nullable: Safe, no data loss
         *
         * 3. The strict equality check was causing false positives for legitimate imports
         *
         * Therefore, we allow nullability differences and rely on BigQuery's runtime enforcement.
         */
        // Removed: if ($expectedDef->isNullable() !== $actualDef->isNullable()) { return false; }

        $expectedLength = (string) ($expectedDef->getLength() ?? '');
        $actualLength = (string) ($actualDef->getLength() ?? '');

        return $expectedLength === $actualLength;
    }

    /**
     * Normalize BigQuery type to its canonical form.
     *
     * BigQuery supports multiple type aliases that are semantically identical.
     * This method normalizes them to prevent false validation failures.
     *
     * Uses BigQuery's REST_API_TYPES_MAP which maps type aliases to canonical forms:
     * - INT, SMALLINT, BIGINT, TINYINT, BYTEINT → INT64
     * - DECIMAL → NUMERIC
     * - BIGDECIMAL → BIGNUMERIC
     *
     * Additional normalization for INTEGER:
     * - Both INT64 and INTEGER are valid in BigQuery and may be returned by reflection
     * - We normalize both to INTEGER for consistency with BigQuery's storage representation
     *
     * @param string $type The type to normalize
     * @return string The canonical type
     */
    private static function normalizeType(string $type): string
    {
        $normalized = strtoupper($type);

        // Apply REST API normalization first
        $normalized = BigqueryDefinition::REST_API_TYPES_MAP[$normalized] ?? $normalized;

        // BigQuery may return either INT64 or INTEGER for integer types
        // Normalize INT64 to INTEGER to match BigQuery's actual storage representation
        if ($normalized === 'INT64') {
            $normalized = 'INTEGER';
        }

        return $normalized;
    }

    /**
     * Builds error message for column definition mismatch.
     *
     * @param BigqueryColumn $expected Expected column
     * @param BigqueryColumn $actual Actual column
     * @return string Error message
     */
    private function buildDefinitionErrorMessage(
        BigqueryColumn $expected,
        BigqueryColumn $actual,
    ): string {
        /** @var BigqueryDefinition $expectedDef */
        $expectedDef = $expected->getColumnDefinition();
        /** @var BigqueryDefinition $actualDef */
        $actualDef = $actual->getColumnDefinition();

        return sprintf(
            '\'%s\' mapping \'%s\' / \'%s\'',
            $actual->getColumnName(),
            $expectedDef->getSQLDefinition(),
            $actualDef->getSQLDefinition(),
        );
    }

    /**
     * Normalizes columns by name for case-insensitive comparison.
     *
     * @param ColumnCollection $columns Columns to normalize
     * @return array<string, BigqueryColumn> Map of lowercase column names to column objects
     */
    private function normalizeColumnsByName(ColumnCollection $columns): array
    {
        $normalized = [];
        /** @var BigqueryColumn $column */
        foreach ($columns as $column) {
            $normalized[strtolower($column->getColumnName())] = $column;
        }

        return $normalized;
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Db\ImportExport\Storage\Bigquery\SelectSource;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ColumnsMismatchException as DriverColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\TableImportQueryBuilder;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

/**
 * Factory for creating import source objects.
 *
 * This factory determines whether to use a direct Table reference or a SelectSource
 * based on column selection, WHERE filters, LIMIT, and other criteria.
 * It handles the complex logic of including WHERE filter columns in the definition
 * for validation while excluding them from the SELECT list.
 */
final class ImportSourceFactory
{
    private BigQueryClient $bqClient;
    private TableImportQueryBuilder $queryBuilder;

    public function __construct(
        BigQueryClient $bqClient,
        ?TableImportQueryBuilder $queryBuilder = null,
    ) {
        $this->bqClient = $bqClient;
        $this->queryBuilder = $queryBuilder ?? new TableImportQueryBuilder(
            $bqClient,
            new ColumnConverter(),
        );
    }

    /**
     * Creates source configuration from command.
     *
     * This method analyzes the command to determine the appropriate source type
     * (Table or SelectSource) and builds the necessary context information.
     *
     * @param TableImportFromTableCommand $command The import command
     * @return SourceContext Object containing source, filtered definition, and full definition
     * @throws DriverColumnsMismatchException When specified columns don't exist in source table
     */
    public function createFromCommand(
        TableImportFromTableCommand $command,
    ): SourceContext {
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null);

        // Get the full source table definition
        $fullSourceDefinition = $this->getSourceTableDefinition($sourceMapping);

        // Determine which columns to import
        $sourceColumns = $this->extractSourceColumns($sourceMapping, $fullSourceDefinition);

        // Create filtered definition with only selected columns
        $effectiveDefinition = $this->filterSourceDefinition(
            $fullSourceDefinition,
            $sourceColumns,
        );

        // Decide between Table and SelectSource
        $source = $this->createSourceObject(
            $sourceMapping,
            $fullSourceDefinition,
            $effectiveDefinition,
            $sourceColumns,
        );

        return new SourceContext(
            source: $source,
            effectiveDefinition: $effectiveDefinition,
            fullDefinition: $fullSourceDefinition,
            selectedColumns: $sourceColumns,
        );
    }

    /**
     * Gets the full source table definition from BigQuery.
     *
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping configuration
     * @return BigqueryTableDefinition The complete table definition
     */
    private function getSourceTableDefinition(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): BigqueryTableDefinition {
        $sourceDataset = ProtobufHelper::repeatedStringToArray($sourceMapping->getPath());
        assert(isset($sourceDataset[0]), 'TableImportFromTableCommand.source.path is required.');

        $definition = (new BigqueryTableReflection(
            $this->bqClient,
            $sourceDataset[0],
            $sourceMapping->getTableName(),
        ))->getTableDefinition();

        assert($definition instanceof BigqueryTableDefinition);
        return $definition;
    }

    /**
     * Extracts column names from mapping or returns all columns if no mapping specified.
     *
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping configuration
     * @param BigqueryTableDefinition $sourceDefinition Source table definition
     * @return string[] Column names to import
     */
    private function extractSourceColumns(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        BigqueryTableDefinition $sourceDefinition,
    ): array {
        $columns = [];
        $columnMappingsField = $sourceMapping->getColumnMappings();

        if ($columnMappingsField !== null) {
            /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping $mapping */
            foreach ($columnMappingsField->getIterator() as $mapping) {
                $columns[] = $mapping->getSourceColumnName();
            }
        }

        if ($columns !== []) {
            return $columns;
        }

        // Return all columns if no mapping specified
        $allColumns = [];
        /** @var BigqueryColumn $column */
        foreach ($sourceDefinition->getColumnsDefinitions() as $column) {
            $allColumns[] = $column->getColumnName();
        }
        return $allColumns;
    }

    /**
     * Filters source definition to include only specified columns.
     *
     * @param BigqueryTableDefinition $sourceDefinition The complete source definition
     * @param string[] $columns Column names to include in filtered definition
     * @return BigqueryTableDefinition Filtered definition with only specified columns
     * @throws DriverColumnsMismatchException When a specified column doesn't exist
     */
    private function filterSourceDefinition(
        BigqueryTableDefinition $sourceDefinition,
        array $columns,
    ): BigqueryTableDefinition {
        if ($columns === []) {
            return $sourceDefinition;
        }

        $columnMap = $this->buildColumnMap($sourceDefinition);
        $filtered = [];

        foreach ($columns as $columnName) {
            $column = $columnMap[strtolower($columnName)] ?? null;
            if ($column === null) {
                throw new DriverColumnsMismatchException(sprintf(
                    'Column "%s" not found in source table %s.%s.',
                    $columnName,
                    $sourceDefinition->getSchemaName(),
                    $sourceDefinition->getTableName(),
                ));
            }
            $filtered[] = $column;
        }

        return new BigqueryTableDefinition(
            $sourceDefinition->getSchemaName(),
            $sourceDefinition->getTableName(),
            $sourceDefinition->isTemporary(),
            new ColumnCollection($filtered),
            $sourceDefinition->getPrimaryKeysNames(),
        );
    }

    /**
     * Creates the appropriate source object (Table or SelectSource).
     *
     * Determines whether to use a direct Table reference or a SelectSource
     * based on column selection, filters, and other criteria.
     *
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping configuration
     * @param BigqueryTableDefinition $fullDefinition Complete source table definition
     * @param BigqueryTableDefinition $effectiveDefinition Filtered source table definition
     * @param string[] $sourceColumns Selected column names
     * @return SqlSourceInterface Either a Table or SelectSource instance
     */
    private function createSourceObject(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        BigqueryTableDefinition $fullDefinition,
        BigqueryTableDefinition $effectiveDefinition,
        array $sourceColumns,
    ): SqlSourceInterface {
        $isFullColumnSet = $this->isFullColumnSet($sourceColumns, $fullDefinition);

        if ($this->shouldUseSelectSource($sourceMapping, $isFullColumnSet)) {
            return $this->createSelectSource(
                $sourceMapping,
                $fullDefinition,
                $effectiveDefinition,
                $sourceColumns,
            );
        }

        return new Table(
            $effectiveDefinition->getSchemaName(),
            $effectiveDefinition->getTableName(),
            $sourceColumns,
            $effectiveDefinition->getPrimaryKeysNames(),
        );
    }

    /**
     * Creates a SelectSource for queries with WHERE filters, LIMIT, or partial column selection.
     *
     * This method handles the complex case where WHERE filters may reference columns
     * that aren't in the SELECT list. Those columns are included in the definition
     * for validation but excluded from the actual SELECT.
     *
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping configuration
     * @param BigqueryTableDefinition $fullDefinition Complete source table definition
     * @param BigqueryTableDefinition $effectiveDefinition Filtered source table definition
     * @param string[] $sourceColumns Selected column names for SELECT list
     * @return SelectSource The configured SelectSource instance
     */
    private function createSelectSource(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        BigqueryTableDefinition $fullDefinition,
        BigqueryTableDefinition $effectiveDefinition,
        array $sourceColumns,
    ): SelectSource {
        // For WHERE filters, include filter columns in definition for validation
        // but NOT in the actual SELECT list
        $whereFilterColumns = $this->extractWhereFilterColumns($sourceMapping);
        $allColumnsForValidation = array_unique(
            array_merge($sourceColumns, $whereFilterColumns),
        );

        $definitionForQuery = $this->filterSourceDefinition(
            $fullDefinition,
            $allColumnsForValidation,
        );

        $queryResponse = $this->queryBuilder->buildSelectSourceSql(
            $definitionForQuery,
            $sourceColumns, // Only selected columns in SELECT
            $sourceMapping,
        );

        return new SelectSource(
            $queryResponse->getQuery(),
            // @phpstan-ignore-next-line argument.type incompatible types in library for PHPStan
            $queryResponse->getBindings(),
            $sourceColumns,
            [],
            $effectiveDefinition->getPrimaryKeysNames(),
        );
    }

    /**
     * Extracts column names used in WHERE filters.
     *
     * These columns may not be in the SELECT list but need to be included
     * in the table definition for validation purposes.
     *
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping configuration
     * @return string[] Column names used in WHERE filters
     */
    private function extractWhereFilterColumns(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): array {
        $columns = [];
        $whereFilters = $sourceMapping->getWhereFilters();

        if ($whereFilters !== null && $whereFilters->count() > 0) {
            /** @var \Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter $filter */
            foreach ($whereFilters as $filter) {
                $columns[] = $filter->getColumnsName();
            }
        }

        return $columns;
    }

    /**
     * Checks if selected columns match the full table column set in order.
     *
     * This determines whether we can use a simple Table reference or need SelectSource.
     *
     * @param string[] $columns Selected column names
     * @param BigqueryTableDefinition $sourceDefinition Source table definition
     * @return bool True if columns match the full set in order
     */
    private function isFullColumnSet(
        array $columns,
        BigqueryTableDefinition $sourceDefinition,
    ): bool {
        if ($columns === []) {
            return true;
        }

        $sourceColumns = [];
        /** @var BigqueryColumn $column */
        foreach ($sourceDefinition->getColumnsDefinitions() as $column) {
            $sourceColumns[] = strtolower($column->getColumnName());
        }

        if (count($columns) !== count($sourceColumns)) {
            return false;
        }

        foreach ($columns as $index => $columnName) {
            if ($sourceColumns[$index] !== strtolower($columnName)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Determines if SelectSource should be used instead of direct Table reference.
     *
     * SelectSource is needed when:
     * - Not all columns are selected
     * - WHERE filters are present
     * - LIMIT is specified
     * - Time travel (seconds) is used
     *
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping configuration
     * @param bool $isFullColumnSet Whether all columns are selected in order
     * @return bool True if SelectSource should be used
     */
    private function shouldUseSelectSource(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        bool $isFullColumnSet,
    ): bool {
        $whereFilters = $sourceMapping->getWhereFilters();
        $hasWhereFilters = $whereFilters !== null && $whereFilters->count() > 0;

        return !$isFullColumnSet
            || (int) $sourceMapping->getSeconds() > 0
            || (int) $sourceMapping->getLimit() > 0
            || $hasWhereFilters;
    }

    /**
     * Builds a case-insensitive column name map for lookups.
     *
     * @param BigqueryTableDefinition $definition Table definition
     * @return array<string, BigqueryColumn> Map of lowercase column names to column objects
     */
    private function buildColumnMap(BigqueryTableDefinition $definition): array
    {
        $map = [];
        /** @var BigqueryColumn $column */
        foreach ($definition->getColumnsDefinitions() as $column) {
            $map[strtolower($column->getColumnName())] = $column;
        }
        return $map;
    }
}

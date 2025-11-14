<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Keboola\Datatype\Definition\Bigquery as BigqueryDefinition;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ColumnsMismatchException as DriverColumnsMismatchException;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;

/**
 * Service for building destination columns from source table definition and column mapping.
 *
 * This service handles:
 * - Identity mapping (1:1) when no explicit mapping is provided
 * - Column renaming via explicit mappings
 * - Type preservation from source to destination
 * - Validation of mapped columns
 */
final class ColumnMappingService
{
    /**
     * Builds destination column collection from source table definition and mapping.
     *
     * If no column mappings are specified, creates an identity mapping (1:1)
     * where all source columns are copied to destination with same names.
     *
     * If mappings are specified, validates that all source columns exist
     * and creates destination columns with mapped names.
     *
     * @param BigqueryTableDefinition $sourceTableDefinition Source table definition
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping configuration
     * @return ColumnCollection Collection of destination columns
     * @throws DriverColumnsMismatchException When a mapped source column doesn't exist
     */
    public function buildDestinationColumns(
        BigqueryTableDefinition $sourceTableDefinition,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): ColumnCollection {
        $sourceColumns = $this->buildSourceColumnMap($sourceTableDefinition);
        $mappings = $this->extractMappings($sourceMapping);

        // No mapping means 1:1 column copy (identity mapping)
        if ($mappings === []) {
            return $this->buildIdentityMapping($sourceTableDefinition);
        }

        return $this->buildMappedColumns($sourceColumns, $mappings, $sourceTableDefinition);
    }

    /**
     * Builds a case-insensitive source column map for lookups.
     *
     * @param BigqueryTableDefinition $sourceTableDefinition Source table definition
     * @return array<string, BigqueryColumn> Map of lowercase column names to column objects
     */
    private function buildSourceColumnMap(
        BigqueryTableDefinition $sourceTableDefinition,
    ): array {
        $sourceColumns = [];
        /** @var BigqueryColumn $column */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $column) {
            $sourceColumns[strtolower($column->getColumnName())] = $column;
        }
        return $sourceColumns;
    }

    /**
     * Extracts column mappings from source mapping configuration.
     *
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping
     * @return TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] Array of column mappings
     */
    private function extractMappings(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): array {
        $columnMappingsField = $sourceMapping->getColumnMappings();
        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] $mappings */
        $mappings = iterator_to_array($columnMappingsField->getIterator());
        return $mappings;
    }

    /**
     * Builds identity mapping where source columns are copied 1:1 to destination.
     *
     * @param BigqueryTableDefinition $sourceTableDefinition Source table definition
     * @return ColumnCollection Collection with all source columns
     */
    private function buildIdentityMapping(
        BigqueryTableDefinition $sourceTableDefinition,
    ): ColumnCollection {
        $definitions = [];
        /** @var BigqueryColumn $column */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $column) {
            $definitions[] = $this->cloneColumnWithName(
                $column,
                $column->getColumnName(),
            );
        }
        return new ColumnCollection($definitions);
    }

    /**
     * Builds mapped columns with validation.
     *
     * Creates destination columns based on explicit column mappings,
     * validating that all source columns exist.
     *
     * @param array<string, BigqueryColumn> $sourceColumns Map of source columns
     * @param TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] $mappings Column mappings
     * @param BigqueryTableDefinition $sourceTableDefinition Source definition for error messages
     * @return ColumnCollection Collection of mapped destination columns
     * @throws DriverColumnsMismatchException When a source column doesn't exist
     */
    private function buildMappedColumns(
        array $sourceColumns,
        array $mappings,
        BigqueryTableDefinition $sourceTableDefinition,
    ): ColumnCollection {
        $definitions = [];
        $missingColumns = [];

        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping $mapping */
        foreach ($mappings as $mapping) {
            $sourceColumn = $sourceColumns[strtolower($mapping->getSourceColumnName())] ?? null;

            if ($sourceColumn === null) {
                $missingColumns[] = $mapping->getSourceColumnName();
                continue;
            }

            $definitions[] = $this->cloneColumnWithName(
                $sourceColumn,
                $mapping->getDestinationColumnName(),
            );
        }

        if ($missingColumns !== []) {
            throw new DriverColumnsMismatchException(sprintf(
                'Some columns are missing in source table %s.%s. Missing columns: %s',
                $sourceTableDefinition->getSchemaName(),
                $sourceTableDefinition->getTableName(),
                implode(',', $missingColumns),
            ));
        }

        return new ColumnCollection($definitions);
    }

    /**
     * Clones a column with a new name, preserving all type information.
     *
     * Creates a new column with:
     * - Same type as source
     * - Same length, nullable, default as source
     * - fieldAsArray if present in source
     * - New column name
     *
     * @param BigqueryColumn $column Source column to clone
     * @param string $newName New name for the destination column
     * @return BigqueryColumn Cloned column with new name
     */
    private function cloneColumnWithName(
        BigqueryColumn $column,
        string $newName,
    ): BigqueryColumn {
        /** @var BigqueryDefinition $definition */
        $definition = $column->getColumnDefinition();

        $options = [
            'length' => $definition->getLength(),
            'nullable' => $definition->isNullable(),
            'default' => $definition->getDefault(),
        ];

        // Handle fieldAsArray if it exists (for STRUCT/ARRAY types)
        if (method_exists($definition, 'getFieldAsArray')
            && $definition->getFieldAsArray() !== null
        ) {
            $options['fieldAsArray'] = $definition->getFieldAsArray();
        }

        return new BigqueryColumn(
            $newName,
            new BigqueryDefinition(
                $definition->getType(),
                $options,
            ),
        );
    }
}

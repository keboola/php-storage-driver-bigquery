<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Helper\BackendHelper;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand\SourceTableMapping\ColumnMapping;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;

final class StageTableDefinitionFactory
{
    /**
     * @param ColumnMapping[] $columnsMapping
     */
    public static function createStagingTableDefinitionWithMapping(
        BigqueryTableDefinition $destination,
        array $columnsMapping,
    ): BigqueryTableDefinition {
        $newDefinitions = [];
        $primaries = $destination->getPrimaryKeysNames();
        foreach ($columnsMapping as $columnMapping) {
            /** @var BigqueryColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnMapping->getDestinationColumnName()) {
                    // if column exists in destination set destination type
                    $newDefinitions[] = new BigqueryColumn(
                        $columnMapping->getDestinationColumnName(),
                        new Bigquery(
                            $definition->getColumnDefinition()->getType(),
                            [
                                'length' => $definition->getColumnDefinition()->getLength(),
                                'nullable' => !in_array($columnMapping->getDestinationColumnName(), $primaries),
                                'default' => $definition->getColumnDefinition()->getDefault(),
                            ],
                        ),
                    );
                    continue 2;
                }
            }
            // if column doesn't exists in destination set default type
            $newDefinitions[] = self::createVarcharColumn($columnMapping->getDestinationColumnName());
        }

        return new BigqueryTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateStagingTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames(),
        );
    }

    /**
     * @param array<string> $sourceColumnsNames
     */
    public static function createStagingTableDefinition(
        BigqueryTableDefinition $destination,
        array $sourceColumnsNames,
    ): BigqueryTableDefinition {
        $newDefinitions = [];
        // create staging table for source columns in order
        // but with types from destination
        // also maintain source columns order
        foreach ($sourceColumnsNames as $columnName) {
            /** @var BigqueryColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnName) {
                    $length = $definition->getColumnDefinition()->getLength();
                    // if column exists in destination set destination type
                    $newDefinitions[] = new BigqueryColumn(
                        $columnName,
                        new Bigquery(
                            $definition->getColumnDefinition()->getType(),
                            [
                                'length' => $length,
                                'nullable' => true,
                                'default' => $definition->getColumnDefinition()->getDefault(),
                            ],
                        ),
                    );
                    continue 2;
                }
            }
            // if column doesn't exists in destination set default type
            $newDefinitions[] = self::createVarcharColumn($columnName);
        }

        return new BigqueryTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateStagingTableName(),
            true,
            new ColumnCollection($newDefinitions),
            [], // <-- ignore primary keys
        );
    }

    private static function createVarcharColumn(string $columnName): BigqueryColumn
    {
        return new BigqueryColumn(
            $columnName,
            new Bigquery(
                Bigquery::TYPE_STRING,
                [
                    'length' => (string) Bigquery::MAX_LENGTH,
                    'nullable' => true,
                ],
            ),
        );
    }
}

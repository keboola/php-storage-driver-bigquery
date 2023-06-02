<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;

class TableReflectionResponseTransformer
{
    public static function transformTableReflectionToResponse(
        string $dataset,
        TableReflectionInterface $ref
    ): TableInfo {
        $res = new TableInfo();
        $def = $ref->getTableDefinition();

        $columns = new RepeatedField(GPBType::MESSAGE, TableInfo\TableColumn::class);
        /** @var BigqueryColumn $col */
        foreach ($def->getColumnsDefinitions() as $col) {
            /** @var Bigquery $colDef */
            $colDef = $col->getColumnDefinition();

            $colInternal = (new TableInfo\TableColumn())
                ->setName($col->getColumnName())
                ->setType($colDef->getType())
                ->setNullable($colDef->isNullable());

            if ($colDef->getLength() !== null) {
                $colInternal->setLength($colDef->getLength());
            }

            if ($colDef->getDefault() !== null) {
                $colInternal->setDefault($colDef->getDefault());
            }

            $columns[] = $colInternal;
        }
        $res->setColumns($columns);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $dataset;
        $res->setPath($path);
        $res->setTableType($ref->getTableType()->value);
        $res->setTableName($def->getTableName());
        $pk = new RepeatedField(GPBType::STRING);
        foreach ($def->getPrimaryKeysNames() as $col) {
            $pk[] = $col;
        }
        $res->setPrimaryKeysNames($pk);
        return $res;
    }
}

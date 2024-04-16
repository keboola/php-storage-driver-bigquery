<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Backend\BigQuery\Clustering;
use Keboola\StorageDriver\Backend\BigQuery\RangePartitioning;
use Keboola\StorageDriver\Backend\BigQuery\TimePartitioning;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\Table\TableType;

class TableReflectionResponseTransformer
{
    public static function transformTableReflectionToResponse(
        string $dataset,
        BigqueryTableReflection $ref,
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
        $res->setTableName($def->getTableName());
        $pk = new RepeatedField(GPBType::STRING);
        foreach ($def->getPrimaryKeysNames() as $col) {
            $pk[] = $col;
        }
        $res->setPrimaryKeysNames($pk);

        $meta = new TableInfo\BigQueryTableMeta();
        $partitioning = $ref->getPartitioningConfiguration();
        if ($partitioning !== null) {
            if ($partitioning->timePartitioningConfig !== null) {
                $timePartitioning = new TimePartitioning();
                $timePartitioning->setType($partitioning->timePartitioningConfig->type);
                if ($partitioning->timePartitioningConfig->column !== null) {
                    $timePartitioning->setField($partitioning->timePartitioningConfig->column);
                }
                if ($partitioning->timePartitioningConfig->expirationMs !== null) {
                    $timePartitioning->setExpirationMs($partitioning->timePartitioningConfig->expirationMs);
                }
                $meta->setTimePartitioning($timePartitioning);
            }
            if ($partitioning->rangePartitioningConfig !== null) {
                $rangePartitioning = (new RangePartitioning())
                    ->setField($partitioning->rangePartitioningConfig->column)
                    ->setRange((new RangePartitioning\Range())
                        ->setStart($partitioning->rangePartitioningConfig->start)
                        ->setEnd($partitioning->rangePartitioningConfig->end)
                        ->setInterval($partitioning->rangePartitioningConfig->interval));
                $meta->setRangePartitioning($rangePartitioning);
            }
            $meta->setRequirePartitionFilter($partitioning->requirePartitionFilter);
        }
        $clustering = $ref->getClusteringConfiguration();
        if ($clustering !== null) {
            $meta->setClustering(
                (new Clustering())
                    ->setFields($clustering->columns),
            );
        }
        $partitions = [];
        foreach ($ref->getPartitionsList() as $partition) {
            $partitions[] = (new TableInfo\BigQueryTableMeta\Partition())
                ->setLastModifiedTime($partition->lastModifiedTime)
                ->setRowsNumber($partition->rowsNumber)
                ->setPartitionId($partition->partitionId)
                ->setStorageTier($partition->storageTier);
        }
        if (count($partitions) !== 0) {
            $meta->setPartitions($partitions);
        }

        $res->setTableType(match ($ref->getTableType()) {
            TableType::BIGQUERY_EXTERNAL => TableInfo\TableType::EXTERNAL,
            default => TableInfo\TableType::NORMAL,
        }
        );

        $any = new Any();
        $any->pack($meta);
        $res->setMeta($any);

        return $res;
    }
}

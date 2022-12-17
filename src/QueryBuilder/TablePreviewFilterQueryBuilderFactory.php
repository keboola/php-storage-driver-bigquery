<?php

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\StorageDriver\Command\Info\TableInfo;

class TablePreviewFilterQueryBuilderFactory
{
    public function __construct()
    {
    }

    public function create(BigQueryClient $bqClient, ?TableInfo $tableInfo): TablePreviewFilterQueryBuilder
    {
        return new TablePreviewFilterQueryBuilder(
            $bqClient,
            $tableInfo,
            new ColumnConverter(),
        );
    }
}

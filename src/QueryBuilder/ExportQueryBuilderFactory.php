<?php

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\StorageDriver\Command\Info\TableInfo;

class ExportQueryBuilderFactory
{
    public function __construct()
    {
    }

    public function create(BigQueryClient $bqClient, ?TableInfo $tableInfo): ExportQueryBuilder
    {
        return new ExportQueryBuilder(
            $bqClient,
            $tableInfo,
            new ColumnConverter(),
        );
    }
}

<?php

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Google\Cloud\BigQuery\BigQueryClient;

class TableExportFilterQueryBuilderFactory
{
    public function __construct()
    {
    }

    public function create(BigQueryClient $bqClient): TableExportFilterQueryBuilder
    {
        return new TableExportFilterQueryBuilder(
            $bqClient,
            new ColumnConverter(),
        );
    }
}

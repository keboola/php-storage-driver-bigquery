<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;

final class BigQueryContext
{
    public function __construct(
        readonly public BigQueryClient $client,
        readonly public Table $table,
    ) {
    }
}

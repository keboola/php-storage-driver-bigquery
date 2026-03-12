<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateViewHandler;
use Keboola\StorageDriver\Command\Table\CreateViewCommand;

final class ViewBucketTestContext
{
    public function __construct(
        public readonly string $sourceProjectId,
        public readonly string $bucketBaName,
        public readonly string $bucketBbName,
        public readonly string $linkedBucketSchemaName,
        public readonly string $listing,
        public readonly string $viewName,
        public readonly ?string $filteredViewName,
        public readonly string $tableName,
        public readonly BigQueryClient $sourceBqClient,
        public readonly BigQueryClient $targetBqClient,
        public readonly RepeatedField $baPath,
        public readonly CreateViewCommand $createViewCommand,
        public readonly CreateViewHandler $createViewHandler,
    ) {
    }
}

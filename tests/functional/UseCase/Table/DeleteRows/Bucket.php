<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\DeleteRows;

use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class Bucket
{
    public readonly string $name;

    public function __construct(
        public readonly GenericBackendCredentials $credentials,
        public readonly CreateBucketResponse $response,
    ) {
        $this->name = $this->response->getCreateBucketObjectName();
    }
}

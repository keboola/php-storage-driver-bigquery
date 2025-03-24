<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\DeleteRows;

use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class Project
{
    public function __construct(
        public readonly GenericBackendCredentials $credentials,
        public readonly CreateProjectResponse $response,
    ) {
    }
}

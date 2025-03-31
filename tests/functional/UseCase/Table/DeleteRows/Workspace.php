<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\DeleteRows;

use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class Workspace
{
    public readonly string $name;

    public function __construct(
        public readonly GenericBackendCredentials $credentials,
        public readonly CreateWorkspaceResponse $response,
    ) {
        $this->name = $this->response->getWorkspaceObjectName();
    }
}

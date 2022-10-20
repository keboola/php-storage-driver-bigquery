<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

class DropBucketHandle implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropBucketCommand);

        $ignoreErrors = $command->getIgnoreErrors();

        $bigQueryClient = $this->clientManager->getBigQueryClient($credentials);

        $dataset = $bigQueryClient->dataset($command->getBucketObjectName());

        try {
            $dataset->delete(['deleteContents' => $command->getIsCascade()]);
        } catch (\Throwable $e) {
            if (!$ignoreErrors) {
                throw $e;
            }
        }

        return null;
    }
}

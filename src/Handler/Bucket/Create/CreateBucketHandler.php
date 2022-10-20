<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class CreateBucketHandler implements DriverCommandHandlerInterface
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
        assert($command instanceof CreateBucketCommand);

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        $newBucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject($command->getBucketId());

        $bigQueryClient = $this->clientManager->getBigQueryClient($credentials);

        $bigQueryClient->createDataset($newBucketDatabaseName);

        return (new CreateBucketResponse())->setCreateBucketObjectName($newBucketDatabaseName);
    }
}

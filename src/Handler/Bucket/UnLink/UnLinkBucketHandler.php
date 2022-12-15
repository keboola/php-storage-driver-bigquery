<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class UnLinkBucketHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command, // linked bucket
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof UnlinkBucketCommand);

        assert($command->getBucketObjectName() !== '', 'UnlinkBucketCommand.bucketObjectName must be filled in');

        $bqClient = $this->clientManager->getBigQueryClient($credentials);
        $dataset = $bqClient->dataset($command->getBucketObjectName());
        $dataset->delete();

        return null;
    }
}

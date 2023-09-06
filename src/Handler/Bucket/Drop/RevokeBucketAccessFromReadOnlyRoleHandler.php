<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\RevokeBucketAccessFromReadOnlyRoleCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Throwable;

final class RevokeBucketAccessFromReadOnlyRoleHandler implements DriverCommandHandlerInterface
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
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof RevokeBucketAccessFromReadOnlyRoleCommand);

        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        assert(
            $command->getBucketObjectName() !== '',
            'RevokeBucketAccessToReadOnlyRoleCommand.bucketObjectName is required'
        );
        $ignoreErrors = $command->getIgnoreErrors();

        $bigQueryClient = $this->clientManager->getBigQueryClient($credentials);
        // In case of deleting an external bucket, we only need the dataset name.
        // This information is stored in the connection so we just delete the dataset
        $dataset = $bigQueryClient->dataset($command->getBucketObjectName());

        try {
            $dataset->delete();
        } catch (Throwable $e) {
            if (!$ignoreErrors) {
                throw $e;
            }
        }

        return null;
    }
}

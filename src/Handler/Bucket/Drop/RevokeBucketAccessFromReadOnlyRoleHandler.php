<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\RevokeBucketAccessFromReadOnlyRoleCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

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
            $command->getProjectReadOnlyRoleName() !== '',
            'RevokeBucketAccessToReadOnlyRoleCommand.projectReadOnlyRoleName is required'
        );
        assert(
            $command->getBucketObjectName() !== '',
            'RevokeBucketAccessToReadOnlyRoleCommand.bucketObjectName is required'
        );
        assert(
            $command->getBranchId() !== '',
            'GrantBucketAccessToReadOnlyRoleCommand.getBranchId is required'
        );

        $bigQueryClient = $this->clientManager->getBigQueryClient($credentials);

        $stackPrefix = getenv('BQ_STACK_PREFIX');
        if ($stackPrefix === false) {
            $stackPrefix = 'local';
        }

        $nameGenerator = new NameGenerator($stackPrefix);

        $bucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject(
            $command->getBucketObjectName(),
            $command->getBranchId()
        );
        $dataset = $bigQueryClient->dataset($bucketDatabaseName);

        $dataset->delete();

        return null;
    }
}

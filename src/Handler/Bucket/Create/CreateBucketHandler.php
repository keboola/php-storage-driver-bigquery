<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class CreateBucketHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateBucketCommand);
        assert($runtimeOptions->getMeta() === null);

        $credentialsMeta = CredentialsHelper::getBigQueryCredentialsMeta($credentials);

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        $newBucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject(
            $command->getBucketId(),
            $command->getBranchId(),
        );

        $bigQueryClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            iterator_to_array($runtimeOptions->getQueryTags()),
        );

        $bigQueryClient->createDataset(
            $newBucketDatabaseName,
            [
                'location' => $credentialsMeta->getRegion(),
                'retries' => self::DEFAULT_RETRY_OVERRIDE,
            ],
        );

        return (new CreateBucketResponse())->setCreateBucketObjectName($newBucketDatabaseName);
    }
}

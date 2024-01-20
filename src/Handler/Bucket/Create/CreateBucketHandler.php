<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Create;

use Exception;
use Google\Protobuf\Internal\Message;
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

        $commandMeta = $command->getMeta();
        if ($commandMeta === null) {
            throw new Exception('CreateBucketBigqueryMeta is required.');
        }

        $commandMeta = $commandMeta->unpack();
        assert($commandMeta instanceof CreateBucketCommand\CreateBucketBigqueryMeta);
        $region = $commandMeta->getRegion();

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        $newBucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject(
            $command->getBucketId(),
            $command->getBranchId(),
        );

        $bigQueryClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);

        $bigQueryClient->createDataset(
            $newBucketDatabaseName,
            [
                'location' => $region,
            ],
        );

        return (new CreateBucketResponse())->setCreateBucketObjectName($newBucketDatabaseName);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Throwable;

final class DropBucketHandler extends BaseHandler
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
        assert($command instanceof DropBucketCommand);

        assert($runtimeOptions->getMeta() === null);

        $bigQueryClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            iterator_to_array($runtimeOptions->getQueryTags()),
        );

        $dataset = $bigQueryClient->dataset($command->getBucketObjectName());

        $dataset->delete([
            'retries' => self::DEFAULT_RETRY_OVERRIDE,
            'deleteContents' => $command->getIsCascade(),
        ]);

        return null;
    }
}

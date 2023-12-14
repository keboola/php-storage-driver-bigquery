<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Throwable;

final class DropBucketHandle extends BaseHandler
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

        $ignoreErrors = $command->getIgnoreErrors();

        $bigQueryClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);

        $dataset = $bigQueryClient->dataset($command->getBucketObjectName());

        try {
            $dataset->delete(['deleteContents' => $command->getIsCascade()]);
        } catch (Throwable $e) {
            if (!$ignoreErrors) {
                throw $e;
            }
        }

        return null;
    }
}

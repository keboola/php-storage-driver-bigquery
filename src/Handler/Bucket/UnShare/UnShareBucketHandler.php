<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\UnShare;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Bucket\UnshareBucketCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class UnShareBucketHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials, // backend credentials
        Message $command,
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof UnshareBucketCommand);
        assert($runtimeOptions->getMeta() === null);

        assert($command->getBucketShareRoleName() !== '', 'UnlinkBucketCommand.bucketShareRoleName must be filled in');

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);

        $analyticHubClient->deleteListing($command->getBucketShareRoleName());

        return null;
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Link;

use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDataset;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDatasetReference;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class LinkBucketHandler extends BaseHandler
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
        Message $credentials, // main credentials
        Message $command, // linked bucket
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof LinkBucketCommand);
        assert($runtimeOptions->getMeta() === null);

        assert($command->getStackPrefix() !== '', 'LinkBucketCommand.stackPrefix must be filled in');
        assert($command->getTargetProjectId() !== '', 'LinkBucketCommand.targetProjectId must be filled in');
        assert($command->getTargetBucketId() !== '', 'LinkBucketCommand.targetBucketId must be filled in');
        assert($command->getSourceShareRoleName() !== '', 'LinkBucketCommand.sourceShareRoleName must be filled in');

        $credentialsMeta = CredentialsHelper::getBigQueryCredentialsMeta($credentials);

        $listing = $command->getSourceShareRoleName();
        $targetProjectId = $command->getTargetProjectId();
        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        $newBucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject(
            $command->getTargetBucketId(),
            null,
        );

        $datasetReference = new DestinationDatasetReference();
        $datasetReference->setProjectId($targetProjectId);
        $datasetReference->setDatasetId($newBucketDatabaseName);

        $destinationDataset = new DestinationDataset([
            'dataset_reference' => $datasetReference,
            'location' => $credentialsMeta->getRegion(),
        ]);
        $analyticHubClient->subscribeListing($listing, [
            'destinationDataset' => $destinationDataset,
        ]);

        return (new LinkedBucketResponse())->setLinkedBucketObjectName($newBucketDatabaseName);
    }
}

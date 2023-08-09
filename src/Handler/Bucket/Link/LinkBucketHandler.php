<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Link;

use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDataset;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDatasetReference;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class LinkBucketHandler implements DriverCommandHandlerInterface
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
        Message $credentials, // main credentials
        Message $command, // linked bucket
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof LinkBucketCommand);

        assert($command->getStackPrefix() !== '', 'LinkBucketCommand.stackPrefix must be filled in');
        assert($command->getTargetProjectId() !== '', 'LinkBucketCommand.targetProjectId must be filled in');
        assert($command->getTargetBucketId() !== '', 'LinkBucketCommand.targetBucketId must be filled in');
        assert($command->getSourceShareRoleName() !== '', 'LinkBucketCommand.sourceShareRoleName must be filled in');

        $listing = $command->getSourceShareRoleName();
        $targetProjectId = $command->getTargetProjectId();
        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        $newBucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject(
            $command->getTargetBucketId(),
            null
        );

        $datasetReference = new DestinationDatasetReference();
        $datasetReference->setProjectId($targetProjectId);
        $datasetReference->setDatasetId($newBucketDatabaseName);

        $destinationDataset = new DestinationDataset([
            'dataset_reference' => $datasetReference,
            'location' => GCPClientManager::DEFAULT_LOCATION,
        ]);
        $analyticHubClient->subscribeListing($listing, [
            'destinationDataset' => $destinationDataset,
        ]);

        return (new LinkedBucketResponse())->setLinkedBucketObjectName($newBucketDatabaseName);
    }
}

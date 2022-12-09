<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Share;

use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class ShareBucketHandler implements DriverCommandHandlerInterface
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
        Message $credentials, // backend credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ShareBucketCommand);

        assert($command->getSourceProjectId() !== '', 'ShareBucketCommand.sourceProjectId must be filled in');
        assert(
            !str_contains($command->getSourceProjectId(), '/'),
            'ShareBucketCommand.sourceProjectId cannot contain "/"'
        );
        assert(
            $command->getSourceProjectReadOnlyRoleName() !== '',
            'ShareBucketCommand.sourceProjectReadOnlyRoleName must be filled in'
        );
        assert(
            $command->getSourceBucketObjectName() !== '',
            'ShareBucketCommand.sourceBucketObjectName must be filled in'
        );
        assert(
            !str_contains($command->getSourceProjectId(), '/'),
            'ShareBucketCommand.sourceBucketObjectName cannot contain "/"'
        );

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);
        $projectStringId = $command->getSourceProjectId();

        $dataExchangeId = $command->getSourceProjectReadOnlyRoleName();

        $formattedParent = $analyticHubClient->dataExchangeName(
            $projectStringId,
            GCPClientManager::DEFAULT_LOCATION,
            $dataExchangeId
        );
        $listingId = $command->getSourceBucketObjectName();
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf(
                'projects/%s/datasets/%s',
                $projectStringId,
                $listingId
            ),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);
        $createdListing = $analyticHubClient->createListing($formattedParent, $listingId, $listing);

        return (new ShareBucketResponse())->setBucketShareRoleName($createdListing->getName());
    }
}

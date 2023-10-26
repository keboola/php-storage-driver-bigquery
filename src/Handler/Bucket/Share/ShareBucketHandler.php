<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Share;

use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Throwable;

final class ShareBucketHandler extends BaseHandler
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
        assert($command instanceof ShareBucketCommand);
        assert($runtimeOptions->getMeta() === null);

        assert($command->getSourceProjectId() !== '', 'ShareBucketCommand.sourceProjectId must be filled in');
        assert(
            str_contains($command->getSourceProjectId(), '/') === false,
            'ShareBucketCommand.sourceProjectId cannot contain "/"'
        );
        assert(
            $command->getSourceProjectReadOnlyRoleName() !== '',
            'ShareBucketCommand.sourceProjectReadOnlyRoleName must be filled in'
        );
        assert(
            $command->getSourceBucketId() !== '',
            'ShareBucketCommand.sourceBucketId must be filled in'
        );
        assert(
            $command->getSourceBucketObjectName() !== '',
            'ShareBucketCommand.sourceBucketObjectName must be filled in'
        );
        assert(
            str_contains($command->getSourceProjectId(), '/') === false,
            'ShareBucketCommand.sourceBucketObjectName cannot contain "/"'
        );

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);
        $projectStringId = $command->getSourceProjectId();

        $dataExchangeId = $command->getSourceProjectReadOnlyRoleName();

        $formattedParent = $analyticHubClient::dataExchangeName(
            $projectStringId,
            GCPClientManager::DEFAULT_LOCATION,
            $dataExchangeId
        );
        // we are using bucketId which is integer id of bucket in connection
        // this way we are preventing that listing name is too long
        // which could occurred if bucketObjectName is longer than 63 characters
        $listingId = $command->getSourceBucketId();
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf(
                'projects/%s/datasets/%s',
                $projectStringId,
                $command->getSourceBucketObjectName()
            ),
        ]);

        $listingName = $analyticHubClient::listingName(
            $projectStringId,
            GCPClientManager::DEFAULT_LOCATION,
            $dataExchangeId,
            $listingId
        );
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);
        try {
            $listing = $analyticHubClient->createListing($formattedParent, $listingId, $listing);
            $this->logger->debug(sprintf('Listing created: %s', $listing->serializeToJsonString()));
        } catch (Throwable $e) {
            if (!str_contains($e->getMessage(), 'Listing already exists')) {
                throw $e;
            }
            $this->logger->debug(sprintf('Listing already exists: %s', $listingName));
            // if listing already exists ignore this error
            // listing is unique for project as it is created with same name as bucket which is also unique in project
        }

        return (new ShareBucketResponse())->setBucketShareRoleName($listingName);
    }
}

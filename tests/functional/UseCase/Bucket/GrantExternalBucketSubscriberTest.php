<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Cloud\Iam\V1\Binding;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\GrantExternalBucketSubscriberHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\GrantExternalBucketSubscriberPermissionDeniedException;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\GrantExternalBucketSubscriberCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Throwable;

class GrantExternalBucketSubscriberTest extends BaseCase
{
    private GenericBackendCredentials $mainProjectCredentials;

    private GenericBackendCredentials $externalProjectCredentials;

    private CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->mainProjectCredentials = $this->projects[0][0];
        $this->externalProjectCredentials = $this->projects[1][0];
        $this->bucketResponse = $this->createTestBucket($this->externalProjectCredentials);
    }

    public function testGrantSubscriberRole(): void
    {
        $externalBucketName = $this->bucketResponse->getCreateBucketObjectName();
        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);

        [$dataExchange, $createdListing] = $this->createExchangeAndListing(
            $externalAnalyticHubClient,
            $externalBucketName,
        );

        $mainCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
        $subscriberEmail = $mainCredentials['client_email'];

        $handler = new GrantExternalBucketSubscriberHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = (new GrantExternalBucketSubscriberCommand())
            ->setListingName($createdListing->getName())
            ->setSubscriberServiceAccountEmail($subscriberEmail);

        $result = $handler(
            $this->externalProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertNull($result);

        // Verify the subscriber role was actually granted on the listing
        $iamPolicy = $externalAnalyticHubClient->getIamPolicy($createdListing->getName());
        $subscriberMember = 'serviceAccount:' . $subscriberEmail;
        $subscriberRole = 'roles/analyticshub.subscriber';

        $granted = false;
        /** @var Binding $binding */
        foreach ($iamPolicy->getBindings() as $binding) {
            if ($binding->getRole() === $subscriberRole) {
                foreach ($binding->getMembers() as $member) {
                    if ($member === $subscriberMember) {
                        $granted = true;
                        break 2;
                    }
                }
            }
        }

        $this->assertTrue($granted, sprintf(
            'Expected member "%s" to have role "%s" on listing "%s".',
            $subscriberMember,
            $subscriberRole,
            $createdListing->getName(),
        ));
    }

    public function testGrantSubscriberRoleIsIdempotent(): void
    {
        $externalBucketName = $this->bucketResponse->getCreateBucketObjectName();
        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);

        [, $createdListing] = $this->createExchangeAndListing(
            $externalAnalyticHubClient,
            $externalBucketName,
        );

        $mainCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
        $subscriberEmail = $mainCredentials['client_email'];

        $handler = new GrantExternalBucketSubscriberHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = (new GrantExternalBucketSubscriberCommand())
            ->setListingName($createdListing->getName())
            ->setSubscriberServiceAccountEmail($subscriberEmail);

        // First call - grants subscriber
        $handler(
            $this->externalProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        // Second call - should be idempotent and not throw
        $result = $handler(
            $this->externalProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertNull($result);

        // Verify the role is still granted and there are no duplicate bindings
        $iamPolicy = $externalAnalyticHubClient->getIamPolicy($createdListing->getName());
        $subscriberMember = 'serviceAccount:' . $subscriberEmail;
        $subscriberRole = 'roles/analyticshub.subscriber';

        $memberCount = 0;
        /** @var Binding $binding */
        foreach ($iamPolicy->getBindings() as $binding) {
            if ($binding->getRole() === $subscriberRole) {
                foreach ($binding->getMembers() as $member) {
                    if ($member === $subscriberMember) {
                        $memberCount++;
                    }
                }
            }
        }

        $this->assertSame(1, $memberCount, 'Subscriber member should appear exactly once after two identical calls.');
    }

    public function testGrantSubscriberRolePermissionDenied(): void
    {
        $externalBucketName = $this->bucketResponse->getCreateBucketObjectName();
        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);

        [, $createdListing] = $this->createExchangeAndListing(
            $externalAnalyticHubClient,
            $externalBucketName,
        );

        $mainCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
        $subscriberEmail = $mainCredentials['client_email'];

        $handler = new GrantExternalBucketSubscriberHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        // Call the handler with the main project credentials, which have no setIamPolicy
        // permission on the listing owned by the external project — expect PERMISSION_DENIED.
        $command = (new GrantExternalBucketSubscriberCommand())
            ->setListingName($createdListing->getName())
            ->setSubscriberServiceAccountEmail($subscriberEmail);

        try {
            $handler(
                $this->mainProjectCredentials,
                $command,
                [],
                new RuntimeOptions(),
            );
            $this->fail('Expected GrantExternalBucketSubscriberPermissionDeniedException to be thrown.');
        } catch (GrantExternalBucketSubscriberPermissionDeniedException $e) {
            $this->assertSame(
                sprintf(
                    'Permission denied when granting subscriber access on listing "%s". Assign ' .
                    'listingAdmin or custom (with setIamPolicy) role  to the service account and try again.',
                    $createdListing->getName(),
                ),
                $e->getMessage(),
            );
            $this->assertFalse($e->isRetryable());
        }
    }

    /**
     * @return array{DataExchange, Listing}
     */
    private function createExchangeAndListing(
        AnalyticsHubServiceClient $analyticHubClient,
        string $bucketDatabaseName,
    ): array {
        $externalCredentials = CredentialsHelper::getCredentialsArray($this->externalProjectCredentials);
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = str_replace('-', '_', $externalProjectStringId) . '_sub_test';
        $formattedParent = $analyticHubClient->locationName($externalProjectStringId, BaseCase::DEFAULT_LOCATION);

        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);

        try {
            $dataExchangeName = AnalyticsHubServiceClient::dataExchangeName(
                $externalProjectStringId,
                BaseCase::DEFAULT_LOCATION,
                $dataExchangeId,
            );
            $existing = $analyticHubClient->getDataExchange($dataExchangeName);
            $analyticHubClient->deleteDataExchange($existing->getName());
        } catch (Throwable) {
            // ignore - may not exist
        }

        $dataExchange = $analyticHubClient->createDataExchange(
            $formattedParent,
            $dataExchangeId,
            $dataExchange,
        );

        $listingId = str_replace('-', '_', $externalProjectStringId) . '_sub_listing';
        $bqDatasetSource = new BigQueryDatasetSource([
            'dataset' => sprintf(
                'projects/%s/datasets/%s',
                $externalProjectStringId,
                $bucketDatabaseName,
            ),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($bqDatasetSource);
        $listing->setDisplayName($listingId);

        $createdListing = $analyticHubClient->createListing($dataExchange->getName(), $listingId, $listing);

        return [$dataExchange, $createdListing];
    }
}

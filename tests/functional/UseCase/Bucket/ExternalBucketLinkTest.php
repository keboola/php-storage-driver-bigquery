<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Cloud\Iam\V1\Binding;
use Google\Protobuf\Any;
use Google\Service\Exception as GoogleServiceException;
use Google\Service\Iam\CreateRoleRequest;
use Google\Service\Iam\Resource\ProjectsRoles;
use Google\Service\Iam\Role;
use Google\Service\Iam\UndeleteRoleRequest;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Throwable;

class ExternalBucketLinkTest extends BaseCase
{
    private GenericBackendCredentials $mainProjectCredentials;

    private GenericBackendCredentials $ebProducerCredentials;

    private GenericBackendCredentials $linkedCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        // KBC1 = projects[0]: the project that registered the external bucket and wants to link it
        $this->mainProjectCredentials = $this->projects[0][0];

        // project that supposes to link the bucket (KBC2) by sharing from KBC1
        $this->linkedCredentials = $this->projects[1][0];

        // exBQ = projects[2]: the external project that owns the Analytics Hub listing
        $this->ebProducerCredentials = $this->projects[2][0];
    }

    /**
     * Tests that LinkBucketHandler correctly handles an external Analytics Hub listing:
     * - Detects the listing is external (signaled by targetServiceAccountEmail in meta)
     * - Grants roles/analyticshub.subscriber to KBC2's SA on the listing
     * - Subscribes to the listing, creating the linked dataset in the target project
     *
     * Required GCP permissions granted in this test:
     * - KBC1's SA gets roles/analyticshub.subscriber on the exchange (to call subscribeListing)
     * - KBC1's SA gets roles/analyticshub.listingAdmin on the listing (to get/set IAM policy)
     */
    public function testLinkExternalBucket(): void
    {
        $exBqBucketResponse = $this->createTestBucket($this->ebProducerCredentials);
        $exBqDatasetName = $exBqBucketResponse->getCreateBucketObjectName();

        // project where the EB will be registered to and where it is shared from. AKA KBC1
        $mainProjectCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);

        // project where the EB will be linked to. AKA KBC2
        $linkingProjectCredentials = CredentialsHelper::getCredentialsArray($this->linkedCredentials);
        $linkingProjectId = $linkingProjectCredentials['project_id'];

        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $linkedBucketId = $this->getTestHash() . 'in.c-Linked';
        $linkedDatasetName = $nameGenerator->createObjectNameForBucketInProject($linkedBucketId, null);

        $mainProjectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->mainProjectCredentials);
        $linkingProjectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->linkedCredentials);

        // cleanup at beginning - delete linked dataset if it exists from previous failed run
        try {
            if ($mainProjectBqClient->dataset($linkedDatasetName)->exists()) {
                $mainProjectBqClient->dataset($linkedDatasetName)->delete(['deleteContents' => true]);
            }
            if ($linkingProjectBqClient->dataset($linkedDatasetName)->exists()) {
                $linkingProjectBqClient->dataset($linkedDatasetName)->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // 1. register the EB - producer creates an exchange and listing pointing to the external bucket,
        // and set KBC1 as subscriber and listing admin so KBC1 can share and link the bucket

        // exBQ creates exchange + listing pointing to exBQ's dataset
        $exBqAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->ebProducerCredentials);
        [$dataExchange, $createdListing] = $this->prepareExternalBucketForRegistration(
            $exBqAnalyticHubClient,
            $this->ebProducerCredentials,
            $exBqDatasetName,
        );

        // 1.2 exBQ grants roles/analyticshub.subscriber on the exchange to KBC1's SA
        // so KBC1 can call subscribeListing
        $iamExchangePolicy = $exBqAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $exchangeBindings = $iamExchangePolicy->getBindings();
        $exchangeBindings[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . $mainProjectCredentials['client_email']],
        ]);
        $iamExchangePolicy->setBindings($exchangeBindings);
        $exBqAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangePolicy);

        // 1.3 exBQ grants roles/analyticshub.listingAdmin on the listing to KBC1's SA
        // so KBC1 can grant subscriber access to KBC2 (get/set IAM policy on listing)
        $iamListingPolicy = $exBqAnalyticHubClient->getIamPolicy($createdListing->getName());
        $listingBindings = $iamListingPolicy->getBindings();
        $listingBindings[] = new Binding([
            'role' => 'roles/analyticshub.listingAdmin',
            'members' => ['serviceAccount:' . $mainProjectCredentials['client_email']],
        ]);
        $iamListingPolicy->setBindings($listingBindings);
        $exBqAnalyticHubClient->setIamPolicy($createdListing->getName(), $iamListingPolicy);

        /*
         * 2.0. link bucket in KBC2 (linking), passing the listing reference and target SA email in meta
         * to signal LinkBucketHandler it's an external listing
         * sharing bucket from KBC1 is not needed because it is in exchanger from exEB already, and it will use this one
         * We need to do is set KBC2 as a subscriber. And we should be able do that, beacuse we have the listingAdmin
         */
        $linkinProjectServiceAccountEmail = $linkingProjectCredentials['client_email'];

        // KBC1 calls LinkBucketHandler to link the external bucket into KBC1's project
        $handler = new LinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($linkingProjectId)
            ->setTargetBucketId($linkedBucketId)
            ->setSourceShareRoleName($createdListing->getName());

        $meta = new Any();
        $meta->pack(
            (new LinkBucketCommand\LinkBucketBigqueryMeta())
                ->setTargetServiceAccountEmail($linkinProjectServiceAccountEmail),
        );
        $command->setMeta($meta);

        /** @var LinkedBucketResponse $result */
        $result = $handler(
            // using the root credentials of both projects
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(LinkedBucketResponse::class, $result);
        $this->assertSame($linkedDatasetName, $result->getLinkedBucketObjectName());

        // Verify the linked dataset was created in KBC1's project
        $this->assertTrue($linkingProjectBqClient->dataset($linkedDatasetName)->exists());

        // Verify KBC2's SA now has roles/analyticshub.subscriber on the listing
        $updatedListingPolicy = $exBqAnalyticHubClient->getIamPolicy($createdListing->getName());
        $subscriberGranted = false;
        /** @var Binding $binding */
        foreach ($updatedListingPolicy->getBindings() as $binding) {
            if ($binding->getRole() === 'roles/analyticshub.subscriber') {
                foreach ($binding->getMembers() as $member) {
                    if ($member === 'serviceAccount:' . $linkinProjectServiceAccountEmail) {
                        $subscriberGranted = true;
                        break 2;
                    }
                }
            }
        }
        $this->assertTrue(
            $subscriberGranted,
            sprintf(
                "KBC2's SA '%s' should have roles/analyticshub.subscriber on the listing",
                $linkinProjectServiceAccountEmail,
            ),
        );
    }

    /**
     * Tests that LinkBucketHandler works when KBC1's SA has a custom role (instead of
     * roles/analyticshub.listingAdmin) with only getIamPolicy and setIamPolicy permissions on the listing.
     *
     * Required GCP permissions granted in this test:
     * - KBC1's SA gets roles/analyticshub.subscriber on the exchange (to call subscribeListing)
     * - KBC1's SA gets a custom role with analyticshub.listings.getIamPolicy + setIamPolicy on the listing
     */
    public function testLinkExternalBucketWithCustomRole(): void
    {
        // 0. prepare
        $exBqBucketResponse = $this->createTestBucket($this->ebProducerCredentials);
        $exBqDatasetName = $exBqBucketResponse->getCreateBucketObjectName();

        $mainProjectCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
        $linkingProjectCredentials = CredentialsHelper::getCredentialsArray($this->linkedCredentials);
        $linkingProjectId = $linkingProjectCredentials['project_id'];

        $externalCredentials = CredentialsHelper::getCredentialsArray($this->ebProducerCredentials);
        $externalProjectId = $externalCredentials['project_id'];

        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $linkedBucketId = $this->getTestHash() . 'in.c-Linked';
        $linkedDatasetName = $nameGenerator->createObjectNameForBucketInProject($linkedBucketId, null);

        $mainProjectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->mainProjectCredentials);
        $linkingProjectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->linkedCredentials);

        // cleanup at beginning - delete linked dataset if it exists from previous failed run
        try {
            if ($mainProjectBqClient->dataset($linkedDatasetName)->exists()) {
                $mainProjectBqClient->dataset($linkedDatasetName)->delete(['deleteContents' => true]);
            }
            if ($linkingProjectBqClient->dataset($linkedDatasetName)->exists()) {
                $linkingProjectBqClient->dataset($linkedDatasetName)->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // 1. exBQ creates exchange + listing pointing to exBQ's dataset
        $exBqAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->ebProducerCredentials);
        [$dataExchange, $createdListing] = $this->prepareExternalBucketForRegistration(
            $exBqAnalyticHubClient,
            $this->ebProducerCredentials,
            $exBqDatasetName,
        );

        // grant roles/analyticshub.subscriber on the exchange to KBC1's SA
        $iamExchangePolicy = $exBqAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $exchangeBindings = $iamExchangePolicy->getBindings();
        $exchangeBindings[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . $mainProjectCredentials['client_email']],
        ]);
        $iamExchangePolicy->setBindings($exchangeBindings);
        $exBqAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangePolicy);

        // 2. Create a custom role in the exBQ project with only the IAM policy permissions needed
        // (equivalent of a minimal alternative to listingAdmin)
        $customRoleId = 'analyticsHubListingIamMgr';

        $projectsRoles = $this->createProjectsRolesResource($this->ebProducerCredentials);
        $createdRole = $this->createOrRecoverCustomRole(
            $projectsRoles,
            $externalProjectId,
            $customRoleId,
            ['analyticshub.listings.getIamPolicy', 'analyticshub.listings.setIamPolicy'],
        );

        // Grant the custom role (instead of listingAdmin) to KBC1's SA on the listing
        $iamListingPolicy = $exBqAnalyticHubClient->getIamPolicy($createdListing->getName());
        $listingBindings = $iamListingPolicy->getBindings();
        $listingBindings[] = new Binding([
            'role' => $createdRole->getName(),
            'members' => ['serviceAccount:' . $mainProjectCredentials['client_email']],
        ]);
        $iamListingPolicy->setBindings($listingBindings);
        $exBqAnalyticHubClient->setIamPolicy($createdListing->getName(), $iamListingPolicy);

        $linkinProjectServiceAccountEmail = $linkingProjectCredentials['client_email'];

        // 3. link the bucket in KBC2, passing the listing reference and target SA email in meta
        $handler = new LinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($linkingProjectId)
            ->setTargetBucketId($linkedBucketId)
            ->setSourceShareRoleName($createdListing->getName());

        $meta = new Any();
        $meta->pack(
            (new LinkBucketCommand\LinkBucketBigqueryMeta())
                ->setTargetServiceAccountEmail($linkinProjectServiceAccountEmail),
        );
        $command->setMeta($meta);

        /** @var LinkedBucketResponse $result */
        $result = $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        // 4. assertion
        $this->assertInstanceOf(LinkedBucketResponse::class, $result);
        $this->assertSame($linkedDatasetName, $result->getLinkedBucketObjectName());
        $this->assertTrue($linkingProjectBqClient->dataset($linkedDatasetName)->exists());

        // Verify KBC2's SA now has roles/analyticshub.subscriber on the listing
        $updatedListingPolicy = $exBqAnalyticHubClient->getIamPolicy($createdListing->getName());
        $subscriberGranted = false;
        /** @var Binding $binding */
        foreach ($updatedListingPolicy->getBindings() as $binding) {
            if ($binding->getRole() === 'roles/analyticshub.subscriber') {
                foreach ($binding->getMembers() as $member) {
                    if ($member === 'serviceAccount:' . $linkinProjectServiceAccountEmail) {
                        $subscriberGranted = true;
                        break 2;
                    }
                }
            }
        }
        $this->assertTrue(
            $subscriberGranted,
            sprintf(
                "KBC2's SA '%s' should have roles/analyticshub.subscriber on the listing",
                $linkinProjectServiceAccountEmail,
            ),
        );
    }

    private function createProjectsRolesResource(GenericBackendCredentials $credentials): ProjectsRoles
    {
        $iamClient = $this->clientManager->getIamClient($credentials);
        return new ProjectsRoles(
            $iamClient,
            'iam',
            'roles',
            [
                'methods' => [
                    'create' => [
                        'path' => 'v1/{+parent}/roles',
                        'httpMethod' => 'POST',
                        'parameters' => [
                            'parent' => ['location' => 'path', 'type' => 'string', 'required' => true],
                        ],
                    ],
                    'delete' => [
                        'path' => 'v1/{+name}',
                        'httpMethod' => 'DELETE',
                        'parameters' => [
                            'name' => ['location' => 'path', 'type' => 'string', 'required' => true],
                        ],
                    ],
                    'get' => [
                        'path' => 'v1/{+name}',
                        'httpMethod' => 'GET',
                        'parameters' => [
                            'name' => ['location' => 'path', 'type' => 'string', 'required' => true],
                        ],
                    ],
                    'undelete' => [
                        'path' => 'v1/{+name}:undelete',
                        'httpMethod' => 'POST',
                        'parameters' => [
                            'name' => ['location' => 'path', 'type' => 'string', 'required' => true],
                        ],
                    ],
                ],
            ],
        );
    }

    /**
     * Creates a custom IAM role in the given project, or recovers it if soft-deleted.
     *
     * @param string[] $permissions
     */
    private function createOrRecoverCustomRole(
        ProjectsRoles $projectsRoles,
        string $projectId,
        string $roleId,
        array $permissions,
    ): Role {
        $roleName = sprintf('projects/%s/roles/%s', $projectId, $roleId);

        $role = new Role();
        $role->setTitle('Analytics Hub Listing IAM Manager');
            $role->setIncludedPermissions($permissions);
            $role->setStage('GA');

        $createRequest = new CreateRoleRequest();
        $createRequest->setRoleId($roleId);
        $createRequest->setRole($role);

        try {
            return $projectsRoles->create(sprintf('projects/%s', $projectId), $createRequest);
        } catch (GoogleServiceException $e) {
            if ($e->getCode() !== 409) {
                throw $e;
            }
        }

        // Role already exists — undelete if soft-deleted, then return current state
        try {
            return $projectsRoles->undelete($roleName, new UndeleteRoleRequest());
        } catch (Throwable) {
            // Role is not deleted (already active), just return it
        }

        return $projectsRoles->get($roleName);
    }

    /**
     * @return array{DataExchange, Listing}
     */
    private function prepareExternalBucketForRegistration(
        AnalyticsHubServiceClient $externalAnalyticHubClient,
        GenericBackendCredentials $externalProjectCredentials,
        string $bucketDatabaseName,
    ): array {
        $externalCredentials = CredentialsHelper::getCredentialsArray($externalProjectCredentials);
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = str_replace('-', '_', $externalProjectStringId) . '_external';
        $formattedParent = $externalAnalyticHubClient->locationName(
            $externalProjectStringId,
            BaseCase::DEFAULT_LOCATION,
        );

        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);

        try {
            $dataExchangeName = AnalyticsHubServiceClient::dataExchangeName(
                $externalProjectStringId,
                BaseCase::DEFAULT_LOCATION,
                $dataExchangeId,
            );
            $dataExchange = $externalAnalyticHubClient->getDataExchange($dataExchangeName);
            $externalAnalyticHubClient->deleteDataExchange($dataExchange->getName());
        } catch (Throwable) {
            // ignore
        }

        $dataExchange = $externalAnalyticHubClient->createDataExchange(
            $formattedParent,
            $dataExchangeId,
            $dataExchange,
        );

        $listingId = str_replace('-', '_', $externalProjectStringId) . '_listing';
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf(
                'projects/%s/datasets/%s',
                $externalProjectStringId,
                $bucketDatabaseName,
            ),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);

        $createdListing = $externalAnalyticHubClient->createListing($dataExchange->getName(), $listingId, $listing);
        return [$dataExchange, $createdListing];
    }
}

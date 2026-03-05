<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Link;

use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDataset;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDatasetReference;
use Google\Cloud\Iam\V1\Binding;
use Google\Cloud\Iam\V1\Policy;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand\LinkBucketBigqueryMeta;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

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

        // Detect whether this is an external BQ bucket link by checking for a targetServiceAccountEmail
        // in the command meta. External listings (Analytics Hub listings from a user's own project,
        // not KBC-managed) require granting subscriber access to KBC2's SA on the listing before subscribing.
        $commandMeta = $command->getMeta();
        $bqMeta = $commandMeta?->unpack();
        $targetServiceAccountEmail = ($bqMeta instanceof LinkBucketBigqueryMeta)
            ? $bqMeta->getTargetServiceAccountEmail()
            : '';
        $isExternalListing = $targetServiceAccountEmail !== '';

        if ($isExternalListing) {
            // Grant roles/analyticshub.subscriber to KBC2's SA on the listing.
            $subscriberMember = 'serviceAccount:' . $targetServiceAccountEmail;

            $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
                if (in_array($e->getCode(), GCPClientManager::ERROR_CODES_FOR_RETRY_IAM)) {
                    return true;
                }
                return false;
            }, 20);
            $proxy = new RetryProxy($retryPolicy, new ExponentialRandomBackOffPolicy());
            $proxy->call(function () use ($analyticHubClient, $listing, $subscriberMember): void {
                $subscriberRole = 'roles/analyticshub.subscriber';
                $currentPolicy = $analyticHubClient->getIamPolicy($listing);
                $existingBindings = $currentPolicy->getBindings();

                // Check if KBC2 already has the subscriber role binding on this listing
                /** @var Binding $binding */
                foreach ($existingBindings as $binding) {
                    if ($binding->getRole() === $subscriberRole) {
                        foreach ($binding->getMembers() as $member) {
                            if ($member === $subscriberMember) {
                                return; // already granted, nothing to do
                            }
                        }
                    }
                }

                $newBindings = iterator_to_array($existingBindings);
                $newBindings[] = new Binding([
                    'role' => $subscriberRole,
                    'members' => [$subscriberMember],
                ]);
                $newPolicy = new Policy();
                $newPolicy->setBindings($newBindings);
                $newPolicy->setEtag($currentPolicy->getEtag());
                $analyticHubClient->setIamPolicy($listing, $newPolicy);
            });
        }

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

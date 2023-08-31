<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Create;

use Google\ApiCore\ApiException;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDataset;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDatasetReference;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;

final class GrantBucketAccessToReadOnlyRoleHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof GrantBucketAccessToReadOnlyRoleCommand);

        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        assert(
            $command->getProjectReadOnlyRoleName() !== '',
            'GrantBucketAccessToReadOnlyRoleCommand.projectReadOnlyRoleName is required'
        );
        assert(
            $command->getBucketObjectName() !== '',
            'GrantBucketAccessToReadOnlyRoleCommand.bucketObjectName is required'
        );

        $projectCredentials = CredentialsHelper::getCredentialsArray($credentials);

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);

        $stackPrefix = getenv('BQ_STACK_PREFIX');
        if ($stackPrefix === false) {
            $stackPrefix = 'local';
        }

        $nameGenerator = new NameGenerator($stackPrefix);

        $newBucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject(
            $command->getBucketObjectName(),
            $command->getBranchId()
        );

        $datasetReference = new DestinationDatasetReference();
        $datasetReference->setProjectId($projectCredentials['project_id']);
        $datasetReference->setDatasetId($newBucketDatabaseName);

        $destinationDataset = new DestinationDataset([
            'dataset_reference' => $datasetReference,
            'location' => GCPClientManager::DEFAULT_LOCATION,
        ]);

        try {
            $analyticHubClient->subscribeListing($command->getProjectReadOnlyRoleName(), [
                'destinationDataset' => $destinationDataset,
            ]);
        } catch (ApiException $e) {
            throw SubscribeListingPermissionDeniedException::handlePermissionDeniedException(
                $e,
                $command->getBucketObjectName(),
                $command->getProjectReadOnlyRoleName()
            );
        }

        return null;
    }
}

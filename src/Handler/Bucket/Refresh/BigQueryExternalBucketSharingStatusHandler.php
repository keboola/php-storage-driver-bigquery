<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Refresh;

use Google\Auth\Credentials\ServiceAccountCredentials;
use Google\Protobuf\Internal\Message;
use GuzzleHttp\Client as GuzzleClient;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Bucket\BigQueryExternalBucketSharingStatusCommand;
use Keboola\StorageDriver\Command\Bucket\BigQueryExternalBucketSharingStatusResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use RuntimeException;

final class BigQueryExternalBucketSharingStatusHandler extends BaseHandler
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
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof BigQueryExternalBucketSharingStatusCommand);
        assert(
            $command->getDestinationDatasetName() !== '',
            'BigQueryExternalBucketSharingStatusCommand.destinationDatasetName is required',
        );

        $credentialsMeta = CredentialsHelper::getBigQueryCredentialsMeta($credentials);
        $region = $credentialsMeta->getRegion();
        $projectCredentials = CredentialsHelper::getCredentialsArray($credentials);
        $projectId = $projectCredentials['project_id'];
        $destinationDatasetName = $command->getDestinationDatasetName();

        // 1. Get a short-lived OAuth2 access token for the service account
        $saCredentials = new ServiceAccountCredentials(
            'https://www.googleapis.com/auth/cloud-platform',
            $projectCredentials,
        );
        $authToken = $saCredentials->fetchAuthToken();
        assert(is_array($authToken) && isset($authToken['access_token']) && is_string($authToken['access_token']));
        $accessToken = $authToken['access_token'];

        // 2. List subscriptions via REST API to find the one backing destinationDatasetName
        $httpClient = new GuzzleClient();
        $url = sprintf(
            'https://analyticshub.googleapis.com/v1/projects/%s/locations/%s/subscriptions',
            $projectId,
            strtolower($region),
        );
        $response = $httpClient->get($url, [
            'headers' => ['Authorization' => 'Bearer ' . $accessToken],
        ]);

        /** @var array{subscriptions?: array<int, array{listing: string, linkedDatasetMap: array<string, array{linkedDataset: string}>}>} $body */
        $body = json_decode((string) $response->getBody(), true, 512, JSON_THROW_ON_ERROR);

        $listingName = null;
        foreach ($body['subscriptions'] ?? [] as $subscription) {
            foreach ($subscription['linkedDatasetMap'] ?? [] as $entry) {
                // linkedDataset value is in the format "projects/{p}/datasets/{dataset}"
                $linkedDataset = $entry['linkedDataset'] ?? '';
                $parts = explode('/', $linkedDataset);
                $datasetId = end($parts);
                if ($datasetId === $destinationDatasetName) {
                    $listingName = $subscription['listing'];
                    break 2;
                }
            }
        }

        if ($listingName === null) {
            throw new RuntimeException(sprintf(
                'No Analytics Hub subscription found for linked dataset "%s" in project "%s" region "%s".',
                $destinationDatasetName,
                $projectId,
                $region,
            ));
        }

        // 3. Check whether the current SA has setIamPolicy on the listing
        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);
        $permissionsResponse = $analyticHubClient->testIamPermissions(
            $listingName,
            ['analyticshub.listings.setIamPolicy'],
        );
        $grantedPermissions = iterator_to_array($permissionsResponse->getPermissions());
        $sourceListingSharingAllowed = in_array('analyticshub.listings.setIamPolicy', $grantedPermissions, true);

        return (new BigQueryExternalBucketSharingStatusResponse())
            ->setSourceListing($listingName)
            ->setSourceListingSharingAllowed($sourceListingSharingAllowed);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Exception as NativeException;
use Google\Client;
use Google\Exception as GoogleClientException;
use Google\Service\Iam;
use Google\Service\Iam\Resource\ProjectsServiceAccounts;
use Google\Service\Iam\Resource\ProjectsServiceAccountsKeys;
use Google\Service\Iam\ServiceAccount;
use Google_Service_Iam_CreateServiceAccountKeyRequest;
use Google_Service_Iam_CreateServiceAccountRequest;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class IAMServiceWrapper extends Iam
{
    public ProjectsServiceAccounts $projects_serviceAccounts;

    public ProjectsServiceAccountsKeys $projects_serviceAccounts_keys;

    public const PRIVATE_KEY_TYPE = 'TYPE_GOOGLE_CREDENTIALS_FILE';
    public const KEY_DATA_PROPERTY_PRIVATE_KEY = 'private_key';

    /**
     * @param Client|array<string, mixed> $clientOrConfig
     */
    public function __construct(Client|array $clientOrConfig = [], ?string $rootUrl = null)
    {
        parent::__construct($clientOrConfig, $rootUrl);

        // The parent Iam class uses IAM API v2 which no longer includes
        // service account resources. Register them manually using v1 paths.
        $this->projects_serviceAccounts = new ProjectsServiceAccounts(
            $this,
            $this->serviceName,
            'serviceAccounts',
            [
                'methods' => [
                    'create' => [
                        'path' => 'v1/{+name}/serviceAccounts',
                        'httpMethod' => 'POST',
                        'parameters' => [
                            'name' => [
                                'location' => 'path',
                                'type' => 'string',
                                'required' => true,
                            ],
                        ],
                    ],
                    'delete' => [
                        'path' => 'v1/{+name}',
                        'httpMethod' => 'DELETE',
                        'parameters' => [
                            'name' => [
                                'location' => 'path',
                                'type' => 'string',
                                'required' => true,
                            ],
                        ],
                    ],
                    'get' => [
                        'path' => 'v1/{+name}',
                        'httpMethod' => 'GET',
                        'parameters' => [
                            'name' => [
                                'location' => 'path',
                                'type' => 'string',
                                'required' => true,
                            ],
                        ],
                    ],
                    'list' => [
                        'path' => 'v1/{+name}/serviceAccounts',
                        'httpMethod' => 'GET',
                        'parameters' => [
                            'name' => [
                                'location' => 'path',
                                'type' => 'string',
                                'required' => true,
                            ],
                            'pageSize' => [
                                'location' => 'query',
                                'type' => 'integer',
                            ],
                            'pageToken' => [
                                'location' => 'query',
                                'type' => 'string',
                            ],
                        ],
                    ],
                ],
            ],
        );

        $this->projects_serviceAccounts_keys = new ProjectsServiceAccountsKeys(
            $this,
            $this->serviceName,
            'keys',
            [
                'methods' => [
                    'create' => [
                        'path' => 'v1/{+name}/keys',
                        'httpMethod' => 'POST',
                        'parameters' => [
                            'name' => [
                                'location' => 'path',
                                'type' => 'string',
                                'required' => true,
                            ],
                        ],
                    ],
                    'delete' => [
                        'path' => 'v1/{+name}',
                        'httpMethod' => 'DELETE',
                        'parameters' => [
                            'name' => [
                                'location' => 'path',
                                'type' => 'string',
                                'required' => true,
                            ],
                        ],
                    ],
                    'list' => [
                        'path' => 'v1/{+name}/keys',
                        'httpMethod' => 'GET',
                        'parameters' => [
                            'name' => [
                                'location' => 'path',
                                'type' => 'string',
                                'required' => true,
                            ],
                            'keyTypes' => [
                                'location' => 'query',
                                'type' => 'string',
                                'repeated' => true,
                            ],
                        ],
                    ],
                ],
            ],
        );
    }

    public function createServiceAccount(
        string $projectServiceAccountId,
        string $projectName,
    ): ServiceAccount {
        $serviceAccountsService = $this->projects_serviceAccounts;
        $createServiceAccountRequest = new Google_Service_Iam_CreateServiceAccountRequest();

        $createServiceAccountRequest->setAccountId($projectServiceAccountId);
        try {
            return $serviceAccountsService->create($projectName, $createServiceAccountRequest);
        } catch (GoogleClientException $e) {
            throw ExceptionHandler::handleRetryException($e);
        }
    }

    /**
     * @return array{0:string, 1:string, 2:string}
     */
    public function createKeyFileCredentials(
        ServiceAccount $serviceAccount,
    ): array {
        $serviceAccKeysService = $this->projects_serviceAccounts_keys;

        $createServiceAccountKeyRequest = new Google_Service_Iam_CreateServiceAccountKeyRequest();
        $createServiceAccountKeyRequest->setPrivateKeyType(self::PRIVATE_KEY_TYPE);
        $retryPolicy = new SimpleRetryPolicy(10);
        $backOffPolicy = new ExponentialBackOffPolicy();
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $key = $proxy->call(function () use (
            $serviceAccKeysService,
            $serviceAccount,
            $createServiceAccountKeyRequest,
        ) {
            try {
                return $serviceAccKeysService->create($serviceAccount->getName(), $createServiceAccountKeyRequest);
            } catch (GoogleClientException $e) {
                throw ExceptionHandler::handleRetryException($e);
            }
        });
        assert($key instanceof Iam\ServiceAccountKey);

        $json = base64_decode($key->getPrivateKeyData());
        $keyData = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        if (!is_array($keyData)) {
            throw new NativeException('Project key credentials missing.');
        }

        $privateKey = $keyData[self::KEY_DATA_PROPERTY_PRIVATE_KEY];
        unset($keyData[self::KEY_DATA_PROPERTY_PRIVATE_KEY]);
        $publicPart = json_encode($keyData);
        assert($publicPart !== false);

        return [$privateKey, $publicPart, $key->name];
    }
}

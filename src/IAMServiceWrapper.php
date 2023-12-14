<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Exception as NativeException;
use Google\Exception as GoogleClientException;
use Google\Service\Iam;
use Google\Service\Iam\ServiceAccount;
use Google_Service_Iam_CreateServiceAccountKeyRequest;
use Google_Service_Iam_CreateServiceAccountRequest;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class IAMServiceWrapper extends Iam
{
    public const PRIVATE_KEY_TYPE = 'TYPE_GOOGLE_CREDENTIALS_FILE';
    public const KEY_DATA_PROPERTY_PRIVATE_KEY = 'private_key';

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
     * @return array{0:string, 1:string}
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

        return [$privateKey, $publicPart];
    }
}

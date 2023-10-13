<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Exception as GoogleClientException;
use Google\Service\Iam;
use Google\Service\Iam\ServiceAccount;
use Google_Service_Iam_CreateServiceAccountKeyRequest;
use Google_Service_Iam_CreateServiceAccountRequest;
use Exception as NativeException;

class IAMServiceWrapper extends Iam
{

    public const PRIVATE_KEY_TYPE = 'TYPE_GOOGLE_CREDENTIALS_FILE';
    public const KEY_DATA_PROPERTY_PRIVATE_KEY = 'private_key';

    public function createServiceAccount(
        string $projectServiceAccountId,
        string $projectName
    ): ServiceAccount
    {
        $serviceAccountsService = $this->projects_serviceAccounts;
        $createServiceAccountRequest = new Google_Service_Iam_CreateServiceAccountRequest();

        $createServiceAccountRequest->setAccountId($projectServiceAccountId);
        try {
            return $serviceAccountsService->create($projectName, $createServiceAccountRequest);
        } catch (GoogleClientException $e) {
            throw ExceptionHandler::handleRetryException($e);
        }
    }

    public function createKeyFileCredentials(
        ServiceAccount $serviceAccount
    ): array
    {
        $serviceAccKeysService = $this->projects_serviceAccounts_keys;

        $createServiceAccountKeyRequest = new Google_Service_Iam_CreateServiceAccountKeyRequest();
        $createServiceAccountKeyRequest->setPrivateKeyType(self::PRIVATE_KEY_TYPE);
        try {
            $key = $serviceAccKeysService->create($serviceAccount->getName(), $createServiceAccountKeyRequest);
        } catch (GoogleClientException $e) {
            throw ExceptionHandler::handleRetryException($e);
        }

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

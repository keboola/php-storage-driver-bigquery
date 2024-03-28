<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials\BigQueryCredentialsMeta;

final class CredentialsHelper
{
    /**
     * @return array{
     * type: string,
     * project_id: string,
     * private_key_id: string,
     * private_key: string,
     * client_email: string,
     * client_id: string,
     * auth_uri: string,
     * token_uri: string,
     * auth_provider_x509_cert_url: string,
     * client_x509_cert_url: string,
     * }
     */
    public static function getCredentialsArray(GenericBackendCredentials $credentials): array
    {
        /**
         * @var array{
         * type: string,
         * project_id: string,
         * private_key_id: string,
         * private_key: string,
         * client_email: string,
         * client_id: string,
         * auth_uri: string,
         * token_uri: string,
         * auth_provider_x509_cert_url: string,
         * client_x509_cert_url: string,
         * } $credentialsArr
         */
        $credentialsArr = (array) json_decode($credentials->getPrincipal(), true, 512, JSON_THROW_ON_ERROR);
        $credentialsArr['private_key'] = $credentials->getSecret();

        return $credentialsArr;
    }

    /**
     * @throws CredentialsMetaRequiredException
     */
    public static function getBigQueryCredentialsMeta(GenericBackendCredentials $credentials): BigQueryCredentialsMeta
    {
        $credentialsMeta = $credentials->getMeta();
        if ($credentialsMeta === null) {
            throw new CredentialsMetaRequiredException();
        }
        $credentialsMeta = $credentialsMeta->unpack();

        assert($credentialsMeta instanceof BigQueryCredentialsMeta);

        return $credentialsMeta;
    }
}

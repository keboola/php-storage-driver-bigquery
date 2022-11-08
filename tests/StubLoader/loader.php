<?php

declare(strict_types=1);

use Keboola\StorageDriver\TestsStubLoader\GCSLoader;

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/../vendor/autoload.php';

switch ($argv[1]) {
    case 'gcs':
        require_once 'GCSLoader.php';

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
         * } $credentials
         */
        $credentials = json_decode((string) getenv('BQ_KEY_FILE'), true, 512, JSON_THROW_ON_ERROR);
        $loader = new GCSLoader(
            $credentials,
            (string) getenv('BQ_BUCKET_NAME'),
        );
        $loader->clearBucket();
        $loader->load();
        break;
    default:
        throw new Exception('Only gcs options are supported.');
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Credentials\BigQueryCredentials;
use LogicException;
use PHPUnit\Framework\TestCase;

class BaseCase extends TestCase
{
    protected GCPClientManager $clientManager;

    /**
     * @param array<mixed> $data
     * @param int|string $dataName
     */
    public function __construct(?string $name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->clientManager = new GCPClientManager();
    }

    /**
     * Get credentials from envs
     */
    protected function getCredentials(): BigQueryCredentials
    {
        $keyFileJson = getenv('BQ_KEYFILE_JSON');
        if ($keyFileJson === false) {
            throw new LogicException('Env "BQ_KEYFILE_JSON" is empty');
        }

        $folderId = (string) getenv('BQ_FOLDER_ID');
        if ($folderId === '') {
            throw new LogicException('Env "BQ_FOLDER_ID" is empty');
        }

        /** @var array<string, string> $credentialsArr */
        $credentialsArr = json_decode($keyFileJson, true);

        return (new BigQueryCredentials($credentialsArr))->setFolderId($folderId);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Google\Protobuf\Any;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
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
    protected function getCredentials(): GenericBackendCredentials
    {
        $principal = getenv('BQ_PRINCIPAL');
        if ($principal === false) {
            throw new LogicException('Env "BQ_PRINCIPAL" is empty');
        }

        $secret = getenv('BQ_SECRET');
        if ($secret === false) {
            throw new LogicException('Env "BQ_SECRET" is empty');
        }
        $secret = str_replace("\\n", "\n", $secret);

        $folderId = (string) getenv('BQ_FOLDER_ID');
        if ($folderId === '') {
            throw new LogicException('Env "BQ_FOLDER_ID" is empty');
        }

        $any = new Any();
        $any->pack((new GenericBackendCredentials\BigQueryCredentialsMeta())->setFolderId(
            $folderId
        ));
        return (new GenericBackendCredentials())
            ->setPrincipal($principal)
            ->setSecret($secret)
            ->setMeta($any);
    }
}

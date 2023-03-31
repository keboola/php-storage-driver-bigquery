<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Platforms\AbstractPlatform;

class FakeDriver implements Driver
{
    /**
     * @param $params array{
     *     'host':string,
     *     'user':string,
     *     'password':string,
     *     'port'?:string,
     *     'warehouse'?:string,
     *     'database'?:string,
     *     'schema'?:string,
     *     'tracing'?:int,
     *     'loginTimeout'?:int,
     *     'networkTimeout'?:int,
     *     'queryTimeout'?: int,
     *     'clientSessionKeepAlive'?: bool,
     *     'maxBackoffAttempts'?:int
     * }
     */
    public function connect(
        array $params
    ): FakeConnection {
        return new FakeConnection();
    }

    public function getDatabasePlatform(): FakePlatform
    {
        return new FakePlatform();
    }

    public function getSchemaManager(Connection $conn, AbstractPlatform $platform): FakeSchemaManager
    {
        assert($platform instanceof FakePlatform);
        return new FakeSchemaManager($conn, $platform);
    }

    public function getExceptionConverter(): FakeExceptionConverter
    {
        return new FakeExceptionConverter();
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;

class FakeConnectionFactory
{
    /**
     * @param array{
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
     * } $params
     */
    public static function getConnection(
        string $host = 'fakehost',
        string $user = 'fakeuser',
        string $password = 'fakepassword',
        array $params = [],
        ?Configuration $config = null
    ): Connection {
        /** @var array{
         *     'port'?:string,
         *     'warehouse'?:string,
         *     'database'?:string,
         *     'schema'?:string,
         *     'tracing'?:int,
         *     'loginTimeout'?:int,
         *     'networkTimeout'?:int,
         *     'queryTimeout'?: int,
         *     'clientSessionKeepAlive'?: bool,
         *     'maxBackoffAttempts'?:int,
         *     'driverClass': class-string<Doctrine\DBAL\Driver>,
         *     'host': string,
         *     'user': string,
         *     'password': string,
         * } $params */
        $params = array_merge(
            $params,
            [
                'driverClass' => FakeDriver::class,
                'host' => $host,
                'user' => $user,
                'password' => $password,
            ]
        );
        return DriverManager::getConnection(
            $params,
            $config
        );
    }
}

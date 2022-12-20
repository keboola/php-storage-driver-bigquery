<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection;

use Doctrine\DBAL\Cache\ArrayResult;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\ParameterType;
use Exception;

/**
 * @method object getNativeConnection()
 */
class FakeConnection implements Connection
{
    public function query(string $sql): ArrayResult
    {
        $stmt = $this->prepare($sql);
        return $stmt->execute();
    }

    public function prepare(string $sql): FakeStatement
    {
        return new FakeStatement();
    }

    /**
     * @inheritDoc
     */
    public function quote($value, $type = ParameterType::STRING): string
    {
        assert(is_string($value));
        return $value;
    }

    public function exec(string $sql): int
    {
        $stmt = $this->prepare($sql);
        $result = $stmt->execute();

        return $result->rowCount();
    }

    /**
     * @inheritDoc
     */
    public function lastInsertId($name = null)
    {
        throw new Exception('method is not implemented yet');
    }

    public function beginTransaction(): bool
    {
        throw new Exception('method is not implemented yet');
    }

    public function commit(): bool
    {
        throw new Exception('method is not implemented yet');
    }

    public function rollBack(): bool
    {
        throw new Exception('method is not implemented yet');
    }
}

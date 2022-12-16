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
    /** @var resource */
    private $conn;

    private array $queries = [];

    /**
     * @param mixed[]|null $options
     */
    public function __construct()
    {
        // do nothing
    }

    public function getPreparedQueries(): array
    {
        return $this->queries;
    }

    public function query(string $sql): ArrayResult
    {
        $stmt = $this->prepare($sql);
        return $stmt->execute();
    }

    public function prepare(string $sql): FakeStatement
    {
        $this->queries[] = $sql;

        return new FakeStatement($this->conn, $sql);
    }

    /**
     * @inheritDoc
     */
    public function quote($value, $type = ParameterType::STRING): string
    {
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

    private function inTransaction(): bool
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

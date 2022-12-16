<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection;

use Doctrine\DBAL\Cache\ArrayResult;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;

class FakeStatement implements Statement
{
    /**
     * @var resource
     */
    private $dbh;

    /**
     * @var resource
     */
    private $stmt;

    /**
     * @var array<mixed>
     */
    private array $params = [];

    private string $query;

    /**
     * @param resource $dbh database handle
     */
    public function __construct($dbh, string $query)
    {
        $this->dbh = $dbh;
        $this->query = $query;
    }

    /**
     * @inheritDoc
     */
    public function bindValue($param, $value, $type = ParameterType::STRING): bool
    {
        return $this->bindParam($param, $value, $type);
    }

    /**
     * @inheritDoc
     */
    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null): bool
    {
        $this->params[$param] = &$variable;
        return true;
    }

    /**
     * @inheritDoc
     */
    public function execute($params = null): ArrayResult
    {
        return new ArrayResult([]);
    }
}

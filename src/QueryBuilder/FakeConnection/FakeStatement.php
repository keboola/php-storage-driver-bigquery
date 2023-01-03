<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection;

use Doctrine\DBAL\Cache\ArrayResult;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;

class FakeStatement implements Statement
{
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

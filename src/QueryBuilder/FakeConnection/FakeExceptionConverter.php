<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection;

use Doctrine\DBAL\Driver\API\ExceptionConverter;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Query;

class FakeExceptionConverter implements ExceptionConverter
{
    public function convert(Exception $exception, ?Query $query): DriverException
    {
        return new DriverException($exception, $query);
    }
}

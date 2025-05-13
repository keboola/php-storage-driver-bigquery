<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

final class IntegerColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_integer_test';
    private const COLUMN_INT_NOT_NULLABLE = 'int_not_nullable';
    private const COLUMN_INT_NULLABLE = 'int_nullable';
    private const COLUMN_STRING_NOT_NULLABLE = 'string_not_nullable';
    private const COLUMN_STRING_NULLABLE = 'string_nullable';

    private const TABLE_STRUCTURE = [
        'columns' => [
            self::COLUMN_INT_NOT_NULLABLE => [
                'type' => Bigquery::TYPE_INT64,
                'nullable' => false,
            ],
            self::COLUMN_INT_NULLABLE => [
                'type' => Bigquery::TYPE_INT64,
                'nullable' => true,
            ],
            self::COLUMN_STRING_NOT_NULLABLE => [
                'type' => Bigquery::TYPE_STRING,
                'nullable' => false,
            ],
            self::COLUMN_STRING_NULLABLE => [
                'type' => Bigquery::TYPE_STRING,
                'nullable' => true,
            ],
        ],
    ];

    private BigQueryClient $bigQuery;

    private Table $table;

    protected function setUp(): void
    {
        parent::setUp();
        $projectCredentials = $this->projects[0][0];

        $bucketName = $this->createTestBucket($projectCredentials)->getCreateBucketObjectName();
        $this->createTable($projectCredentials, $bucketName, self::TABLE_NAME, self::TABLE_STRUCTURE);

        $this->bigQuery = $this->clientManager->getBigQueryClient($this->testRunId, $projectCredentials);
        $this->table = $this->bigQuery->dataset($bucketName)->table(self::TABLE_NAME);

        $this->bigQuery->runQuery($this->bigQuery->query(sprintf(
            <<<'SQL'
                INSERT INTO `%s.%s` (%s, %s, %s, %s) VALUES
                (1, 1, '1', '1'),
                (2, 2, '2', '2'),
                (3, NULL, '3', NULL),
                (3, NULL, '3', NULL),
                (4, 4, '4', '4'),
                (5, 5, '5', '5'),
                (5, 5, '5', '5'),
                (100, 100, '100', '100'),
                (0, 0, '0', '0'),
                (-10, -10, '-10', '-10'),
                (999999, NULL, '999999', NULL);
                SQL,
            $bucketName,
            self::TABLE_NAME,
            self::COLUMN_INT_NOT_NULLABLE,
            self::COLUMN_INT_NULLABLE,
            self::COLUMN_STRING_NOT_NULLABLE,
            self::COLUMN_STRING_NULLABLE,
        )));
    }
}

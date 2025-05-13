<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

final class DateColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_date_test';
    private const COLUMN_DATE_NOT_NULLABLE = 'date_not_nullable';
    private const COLUMN_DATE_NULLABLE = 'date_nullable';
    private const COLUMN_STRING_NOT_NULLABLE = 'string_not_nullable';
    private const COLUMN_STRING_NULLABLE = 'string_nullable';

    private const TABLE_STRUCTURE = [
        'columns' => [
            self::COLUMN_DATE_NOT_NULLABLE => [
                'type' => Bigquery::TYPE_DATE,
                'nullable' => false,
            ],
            self::COLUMN_DATE_NULLABLE => [
                'type' => Bigquery::TYPE_DATE,
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
                (DATE '2023-01-01', DATE '2023-01-01', '2023-01-01', '2023-01-01'),
                (DATE '2023-01-02', NULL, '2023-01-02', NULL),
                (DATE '2023-01-02', NULL, '2023-01-02', NULL),
                (DATE '2023-01-03', DATE '2023-01-03', '2023-01-03', '2023-01-03'),
                (DATE '2023-01-03', DATE '2023-01-03', '2023-01-03', '2023-01-03'),
                (DATE '2022-12-31', DATE '2022-12-31', '2022-12-31', '2022-12-31'),
                (DATE '2024-02-29', DATE '2024-02-29', '2024-02-29', '2024-02-29'),
                (DATE '2023-12-25', NULL, '2023-12-25', NULL),
                (DATE '1991-12-02', DATE '1991-12-02', '1991-12-02', '1991-12-02');
                SQL,
            $bucketName,
            self::TABLE_NAME,
            self::COLUMN_DATE_NOT_NULLABLE,
            self::COLUMN_DATE_NULLABLE,
            self::COLUMN_STRING_NOT_NULLABLE,
            self::COLUMN_STRING_NULLABLE,
        )));
    }
}

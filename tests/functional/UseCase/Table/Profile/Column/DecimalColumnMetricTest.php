<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountMetric;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

final class DecimalColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_decimal_test';
    private const COLUMN_DECIMAL_NOT_NULLABLE = 'decimal_not_nullable';
    private const COLUMN_DECIMAL_NULLABLE = 'decimal_nullable';
    private const COLUMN_STRING_NOT_NULLABLE = 'string_not_nullable';
    private const COLUMN_STRING_NULLABLE = 'string_nullable';

    private const TABLE_STRUCTURE = [
        'columns' => [
            self::COLUMN_DECIMAL_NOT_NULLABLE => [
                'type' => Bigquery::TYPE_DECIMAL,
                'nullable' => false,
            ],
            self::COLUMN_DECIMAL_NULLABLE => [
                'type' => Bigquery::TYPE_DECIMAL,
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

    public function testDistinctCountNotNullable(): void
    {
        $metric = new DistinctCountMetric();
        $countDecimal = $metric->collect(self::COLUMN_DECIMAL_NOT_NULLABLE, $this->table, $this->bigQuery);
        $countString = $metric->collect(self::COLUMN_STRING_NOT_NULLABLE, $this->table, $this->bigQuery);

        $this->assertSame(7, $countDecimal);
        $this->assertSame(7, $countString);
    }

    public function testDistinctCountNullable(): void
    {
        $metric = new DistinctCountMetric();
        $countDecimal = $metric->collect(self::COLUMN_DECIMAL_NULLABLE, $this->table, $this->bigQuery);
        $countString = $metric->collect(self::COLUMN_STRING_NULLABLE, $this->table, $this->bigQuery);

        $this->assertSame(5, $countDecimal);
        $this->assertSame(5, $countString);
    }

    public function testDuplicateCountNotNullable(): void
    {
        $metric = new DuplicateCountMetric();
        $countDecimal = $metric->collect(self::COLUMN_DECIMAL_NOT_NULLABLE, $this->table, $this->bigQuery);
        $countString = $metric->collect(self::COLUMN_STRING_NOT_NULLABLE, $this->table, $this->bigQuery);

        $this->assertSame(2, $countDecimal);
        $this->assertSame(2, $countString);
    }

    public function testDuplicateCountNullable(): void
    {
        $metric = new DuplicateCountMetric();
        $countDecimal = $metric->collect(self::COLUMN_DECIMAL_NULLABLE, $this->table, $this->bigQuery);
        $countString = $metric->collect(self::COLUMN_STRING_NULLABLE, $this->table, $this->bigQuery);

        $this->assertSame(1, $countDecimal);
        $this->assertSame(1, $countString);
    }

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
                (10.5, 10.5, '10.5', '10.5'),
                (10.5, 10.5, '10.5', '10.5'),
                (20.0, NULL, '20.0', NULL),
                (20.0, NULL, '20.0', NULL),
                (-5.25, -5.25, '-5.25', '-5.25'),
                (0.00, 0.00, '0.00', '0.00'),
                (9999999999.999999999, 9999999999.999999999, '9999999999.999999999', '9999999999.999999999'),
                (3.141592, NULL, '3.141592', NULL),
                (1.0, 1.0, '1.0', '1.0');
                SQL,
            $bucketName,
            self::TABLE_NAME,
            self::COLUMN_DECIMAL_NOT_NULLABLE,
            self::COLUMN_DECIMAL_NULLABLE,
            self::COLUMN_STRING_NOT_NULLABLE,
            self::COLUMN_STRING_NULLABLE,
        )));
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountMetric;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

final class StringColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_string_test';
    private const COLUMN_NOT_NULLABLE = 'string_not_nullable';
    private const COLUMN_NULLABLE = 'string_nullable';

    private const TABLE_STRUCTURE = [
        'columns' => [
            self::COLUMN_NOT_NULLABLE => [
                'type' => Bigquery::TYPE_STRING,
                'nullable' => false,
            ],
            self::COLUMN_NULLABLE => [
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
        $count = $metric->collect(self::COLUMN_NOT_NULLABLE, $this->table, $this->bigQuery);

        $this->assertSame(21, $count);
    }

    public function testDistinctCountNullable(): void
    {
        $metric = new DistinctCountMetric();
        $count = $metric->collect(self::COLUMN_NULLABLE, $this->table, $this->bigQuery);

        $this->assertSame(14, $count);
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
                INSERT INTO `%s.%s` (%s, %s) VALUES
                ("alpha", "alpha"),
                ("beta", "beta"),
                ("gamma", "gamma"),
                ("delta", "delta"),
                ("delta", NULL),
                ("delta", NULL),
                ("", ""),
                ("", NULL),
                ("a very long string value here", "a very very long string value here"),
                ("český", "český"),
                ("@special!", "@special!"),
                ("12345", "12345"),
                ("user@example.com", "user@example.com"),
                ("admin@test.org", "admin@test.org"),
                ("omega", "omega"),
                ("omega", "omega"),
                ("more-data", NULL),
                ("test-value", NULL),
                ("xyz", NULL),
                ("duplicate-test", "duplicate-test"),
                ("duplicate-test", "duplicate-test"),
                ("unique-1", "unique-1"),
                ("unique-2", NULL),
                ("unique-3", ""),
                ("empty-again", ""),
                ("null-and-empty", NULL)
                SQL,
            $bucketName,
            self::TABLE_NAME,
            self::COLUMN_NOT_NULLABLE,
            self::COLUMN_NULLABLE,
        )));
    }
}

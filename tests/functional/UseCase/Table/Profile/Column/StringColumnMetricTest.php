<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

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

    private string $dataset;

    private BigQueryContext $context;

    public function testDistinctCountNotNullable(): void
    {
        $metric = new DistinctCountColumnMetric();

        $count = $metric->collect($this->dataset, self::TABLE_NAME, self::COLUMN_NOT_NULLABLE, $this->context);
        $this->assertSame(21, $count);
    }

    public function testDistinctCountNullable(): void
    {
        $metric = new DistinctCountColumnMetric();

        $count = $metric->collect($this->dataset, self::TABLE_NAME, self::COLUMN_NULLABLE, $this->context);
        $this->assertSame(14, $count);
    }

    public function testDuplicateCountNotNullable(): void
    {
        $metric = new DuplicateCountColumnMetric();

        $count = $metric->collect($this->dataset, self::TABLE_NAME, self::COLUMN_NOT_NULLABLE, $this->context);
        $this->assertSame(5, $count);
    }

    public function testDuplicateCountNullable(): void
    {
        $metric = new DuplicateCountColumnMetric();

        $count = $metric->collect($this->dataset, self::TABLE_NAME, self::COLUMN_NULLABLE, $this->context);
        $this->assertSame(4, $count);
    }

    protected function setUp(): void
    {
        parent::setUp();
        $projectCredentials = $this->projects[0][0];

        $this->dataset = $this->createTestBucket($projectCredentials)->getCreateBucketObjectName();
        $this->createTable($projectCredentials, $this->dataset, self::TABLE_NAME, self::TABLE_STRUCTURE);

        $bigQuery = $this->clientManager->getBigQueryClient($this->testRunId, $projectCredentials);
        $this->context = new BigQueryContext(
            $bigQuery,
            $bigQuery->dataset($this->dataset)->table(self::TABLE_NAME),
        );

        $bigQuery->runQuery($bigQuery->query(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (%s, %s) VALUES
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
            BigqueryQuote::quoteSingleIdentifier($this->dataset),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_NAME),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_NULLABLE),
        )));
    }
}

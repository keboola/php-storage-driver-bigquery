<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\Column\AvgMinMaxLengthColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\NullCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
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

    /**
     * @dataProvider metricProvider
     * @param array{avg: float, min: int, max: int}|int $expected
     */
    public function testMetric(
        ColumnMetricInterface $metric,
        string $column,
        array|int $expected,
    ): void {
        $actual = $metric->collect($this->dataset, self::TABLE_NAME, $column, $this->context);
        $this->assertSame($expected, $actual);
    }

    public function metricProvider(): Generator
    {
        yield 'distinctCount (not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_NOT_NULLABLE,
            21,
        ];

        yield 'distinctCount (nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_NULLABLE,
            14,
        ];

        yield 'duplicateCount (not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_NOT_NULLABLE,
            5,
        ];

        yield 'duplicateCount (nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_NULLABLE,
            4,
        ];

        yield 'nullCount (not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_NULLABLE,
            8,
        ];

        yield 'avgMinMaxLength (not nullable)' => [
            new AvgMinMaxLengthColumnMetric(),
            self::COLUMN_NOT_NULLABLE,
            [
                'avg' => 8.3077,
                'min' => 0,
                'max' => 29,
            ],
        ];

        yield 'avgMinMaxLength (nullable)' => [
            new AvgMinMaxLengthColumnMetric(),
            self::COLUMN_NULLABLE,
            [
                'avg' => 8.2222,
                'min' => 0,
                'max' => 34,
            ],
        ];
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

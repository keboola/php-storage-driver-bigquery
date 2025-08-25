<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\NullCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\NumericStatisticsColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

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

    private string $dataset;

    private BigQueryContext $context;

    /**
     * @dataProvider metricProvider
     * @param array<mixed>|int $expected
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
        yield 'distinctCount (int, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_INT_NOT_NULLABLE,
            9,
        ];

        yield 'distinctCount (string, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_STRING_NOT_NULLABLE,
            9,
        ];

        yield 'distinctCount (int, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_INT_NULLABLE,
            7,
        ];

        yield 'distinctCount (string, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_STRING_NULLABLE,
            7,
        ];

        yield 'duplicateCount (int, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_INT_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (string, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_STRING_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (int, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_INT_NULLABLE,
            1,
        ];

        yield 'duplicateCount (string, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_STRING_NULLABLE,
            1,
        ];

        yield 'nullCount (int, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_INT_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (string, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_STRING_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (int, nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_INT_NULLABLE,
            3,
        ];

        yield 'nullCount (string, nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_STRING_NULLABLE,
            3,
        ];

        yield 'numeric statistics (int, not nullable)' => [
            new NumericStatisticsColumnMetric(),
            self::COLUMN_INT_NOT_NULLABLE,
            [
                'avg' => 90919.272727,
                'mode' => 5.0,
                'median' => 3.0,
                'min' => -10.0,
                'max' => 999999.0,
            ],
        ];

        // @todo Waiting for implementation of untyped tables profiling.
//        yield 'numeric statistics (string, not nullable)' => [
//            new NumericStatisticsColumnMetric(),
//            self::COLUMN_STRING_NOT_NULLABLE,
//            [],
//        ];

        yield 'numeric statistics (int, nullable)' => [
            new NumericStatisticsColumnMetric(),
            self::COLUMN_INT_NULLABLE,
            [
                'avg' => 13.375000,
                'mode' => 5.0,
                'median' => 3.0,
                'min' => -10.0,
                'max' => 100.0,
            ],
        ];

        // @todo Waiting for implementation of untyped tables profiling.
//        yield 'numeric statistics (string, nullable)' => [
//            new NumericStatisticsColumnMetric(),
//            self::COLUMN_STRING_NULLABLE,
//            [],
//        ];
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
                INSERT INTO %s.%s (%s, %s, %s, %s) VALUES
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
            BigqueryQuote::quoteSingleIdentifier($this->dataset),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_NAME),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_INT_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_INT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_STRING_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_STRING_NULLABLE),
        )));
    }
}

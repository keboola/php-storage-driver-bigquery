<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class FloatColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_float_test';
    private const COLUMN_FLOAT_NOT_NULLABLE = 'float_not_nullable';
    private const COLUMN_FLOAT_NULLABLE = 'float_nullable';
    private const COLUMN_STRING_NOT_NULLABLE = 'string_not_nullable';
    private const COLUMN_STRING_NULLABLE = 'string_nullable';

    private const TABLE_STRUCTURE = [
        'columns' => [
            self::COLUMN_FLOAT_NOT_NULLABLE => [
                'type' => Bigquery::TYPE_FLOAT64,
                'nullable' => false,
            ],
            self::COLUMN_FLOAT_NULLABLE => [
                'type' => Bigquery::TYPE_FLOAT64,
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
     */
    public function testMetric(
        ColumnMetricInterface $metric,
        string $column,
        int $expected,
    ): void {
        $actual = $metric->collect($this->dataset, self::TABLE_NAME, $column, $this->context);
        $this->assertSame($expected, $actual);
    }

    public function metricProvider(): Generator
    {
        yield 'distinctCount (float, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_FLOAT_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (string, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_STRING_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (float, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_FLOAT_NULLABLE,
            5,
        ];

        yield 'distinctCount (string, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_STRING_NULLABLE,
            5,
        ];

        yield 'duplicateCount (float, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_FLOAT_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (string, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_STRING_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (float, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_FLOAT_NULLABLE,
            1,
        ];

        yield 'duplicateCount (string, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_STRING_NULLABLE,
            1,
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

        // @todo Missing test data for -inf, +inf and NaN values.
        $bigQuery->runQuery($bigQuery->query(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (%s, %s, %s, %s) VALUES
                (1.23, 1.23, '1.23', '1.23'),
                (1.23, 1.23, '1.23', '1.23'),
                (4.56, NULL, '4.56', NULL),
                (4.56, NULL, '4.56', NULL),
                (7.89, 7.89, '7.89', '7.89'),
                (0.0, 0.0, '0.0', '0.0'),
                (-3.21, -3.21, '-3.21', '-3.21'),
                (123456.789, 123456.789, '123456.789', '123456.789'),
                (999.99, NULL, '999.99', NULL);
                SQL,
            BigqueryQuote::quoteSingleIdentifier($this->dataset),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_NAME),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_FLOAT_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_FLOAT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_STRING_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_STRING_NULLABLE),
        )));
    }
}

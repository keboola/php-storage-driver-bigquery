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
        yield 'distinctCount (date, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_DATE_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (string, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_STRING_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (date, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_DATE_NULLABLE,
            5,
        ];

        yield 'distinctCount (string, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_STRING_NULLABLE,
            5,
        ];

        yield 'duplicateCount (date, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_DATE_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (string, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_STRING_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (date, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_DATE_NULLABLE,
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

        $bigQuery->runQuery($bigQuery->query(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (%s, %s, %s, %s) VALUES
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
            BigqueryQuote::quoteSingleIdentifier($this->dataset),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_NAME),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_DATE_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_DATE_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_STRING_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_STRING_NULLABLE),
        )));
    }
}

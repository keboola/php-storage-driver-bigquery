<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

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
        yield 'distinctCount (decimal, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_DECIMAL_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (string, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_STRING_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (decimal, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_DECIMAL_NULLABLE,
            5,
        ];

        yield 'distinctCount (string, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_STRING_NULLABLE,
            5,
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
            BigqueryQuote::quoteSingleIdentifier($this->dataset),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_NAME),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_DECIMAL_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_DECIMAL_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_STRING_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_STRING_NULLABLE),
        )));
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class BooleanColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_boolean_test';
    private const COLUMN_BOOL_NOT_NULLABLE = 'bool_not_nullable';
    private const COLUMN_BOOL_NULLABLE = 'bool_nullable';
//    private const COLUMN_STRING_NOT_NULLABLE = 'string_not_nullable'; @todo Test string columns equivalent to boolean
//    private const COLUMN_STRING_NULLABLE = 'string_nullable';

    private const TABLE_STRUCTURE = [
        'columns' => [
            self::COLUMN_BOOL_NOT_NULLABLE => [
                'type' => Bigquery::TYPE_BOOL,
                'nullable' => false,
            ],
            self::COLUMN_BOOL_NULLABLE => [
                'type' => Bigquery::TYPE_BOOL,
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
        $result = $metric->collect($this->dataset, self::TABLE_NAME, $column, $this->context);
        $this->assertSame($expected, $result);
    }

    public static function metricProvider(): Generator
    {
        yield 'duplicateCount (bool, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_BOOL_NOT_NULLABLE,
            4,
        ];

        yield 'duplicateCount (bool, nullable)' => [
            'metric' => new DuplicateCountColumnMetric(),
            'column' => self::COLUMN_BOOL_NULLABLE,
            'expected' => 2,
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
                (TRUE, TRUE),
                (TRUE, TRUE),
                (FALSE, FALSE),
                (FALSE, FALSE),
                (TRUE, NULL),
                (FALSE, NULL);
                SQL,
            BigqueryQuote::quoteSingleIdentifier($this->dataset),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_NAME),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_BOOL_NOT_NULLABLE),
            BigqueryQuote::quoteSingleIdentifier(self::COLUMN_BOOL_NULLABLE),
        )));
    }
}

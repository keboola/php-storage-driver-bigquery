<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\ColumnCountTableMetric;
use Keboola\StorageDriver\BigQuery\Profile\DataSizeTableMetric;
use Keboola\StorageDriver\BigQuery\Profile\RowCountTableMetric;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class TableMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_table_test';

    private const TABLE_STRUCTURE = [
        'columns' => [
            'id' => [
                'type' => Bigquery::TYPE_INT64,
                'nullable' => false,
            ],
            'name' => [
                'type' => Bigquery::TYPE_STRING,
                'nullable' => true,
            ],
            'age' => [
                'type' => Bigquery::TYPE_INT64,
                'nullable' => true,
            ],
            'signup_date' => [
                'type' => Bigquery::TYPE_DATE,
                'nullable' => true,
            ],
            'email' => [
                'type' => Bigquery::TYPE_STRING,
                'nullable' => true,
            ],
            'isActive' => [
                'type' => Bigquery::TYPE_BOOL,
                'nullable' => true,
            ],
            'score' => [
                'type' => Bigquery::TYPE_FLOAT64,
                'nullable' => true,
            ],
        ],
        'primaryKey' => ['id'],
    ];

    private string $dataset;

    private BigQueryContext $context;

    public function testColumnCount(): void
    {
        $metric = new ColumnCountTableMetric();
        $count = $metric->collect($this->dataset, self::TABLE_NAME, $this->context);

        $this->assertSame(7, $count);
    }

    public function testRowCount(): void
    {
        $metric = new RowCountTableMetric();
        $count = $metric->collect($this->dataset, self::TABLE_NAME, $this->context);

        $this->assertSame(8, $count);
    }

    public function testDataSize(): void
    {
        $metric = new DataSizeTableMetric();
        $bytes = $metric->collect($this->dataset, self::TABLE_NAME, $this->context);

        $this->assertSame(369, $bytes);
    }

    protected function setUp(): void
    {
        parent::setUp();
        $projectCredentials = $this->projects[0][0];

        $this->dataset = $this->createTestBucket($projectCredentials)->getCreateBucketObjectName();
        $this->createTable($projectCredentials, $this->dataset, self::TABLE_NAME, self::TABLE_STRUCTURE);

        $bigQuery  = $this->clientManager->getBigQueryClient($this->testRunId, $projectCredentials);
        $this->context = new BigQueryContext(
            $bigQuery,
            $bigQuery->dataset($this->dataset)->table(self::TABLE_NAME),
        );

        $bigQuery ->runQuery($bigQuery->query(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (`id`, `name`, `age`, `signup_date`, `email`, `isActive`, `score`) VALUES
                (1, "Alice", 30, DATE "2024-01-01", "alice@example.com", TRUE, 85.5),
                (2, "Bob",  NULL, DATE "2024-02-15", "bob@example.com", FALSE, NULL),
                (3, "Charlie", 25, NULL, "charlie@example.com", TRUE, 92.0),
                (4, "Alice", 30, DATE "2024-01-01", "alice@example.com", TRUE, 85.5),
                (5, NULL, NULL, NULL, NULL, NULL, NULL),
                (6, "Eve", 27, DATE "2024-03-01", "eve@example.com", TRUE, 78.3),
                (6, "Eve", 27, DATE "2024-03-01", "eve@example.com", TRUE, 78.3),
                (7, "Bob", NULL, DATE "2024-02-15", "bob@example.com", FALSE, NULL)
                SQL,
            BigqueryQuote::quoteSingleIdentifier($this->dataset),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_NAME),
        )));
    }
}

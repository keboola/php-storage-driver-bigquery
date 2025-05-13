<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Profile\Column;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountMetric;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

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

    private BigQueryClient $bigQuery;

    private Table $table;

    public function testDuplicateCountNotNullable(): void
    {
        $metric = new DuplicateCountMetric();
        $countBool = $metric->collect(self::COLUMN_BOOL_NOT_NULLABLE, $this->table, $this->bigQuery);

        $this->assertSame(4, $countBool);
    }

    public function testDuplicateCountNullable(): void
    {
        $metric = new DuplicateCountMetric();
        $countBool = $metric->collect(self::COLUMN_BOOL_NULLABLE, $this->table, $this->bigQuery);

        $this->assertSame(2, $countBool);
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
                (TRUE, TRUE),
                (TRUE, TRUE),
                (FALSE, FALSE),
                (FALSE, FALSE),
                (TRUE, NULL),
                (FALSE, NULL);
                SQL,
            $bucketName,
            self::TABLE_NAME,
            self::COLUMN_BOOL_NOT_NULLABLE,
            self::COLUMN_BOOL_NULLABLE,
        )));
    }
}

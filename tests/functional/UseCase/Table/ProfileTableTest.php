<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Profile\ProfileTableHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\CreateProfileTableCommand;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse\Column;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

final class ProfileTableTest extends BaseCase
{
    private const TABLE_NAME = 'profile_table_test';

    private const TABLE_STRUCTURE = [
        'columns' => [
            'id' => [
                'type' => Bigquery::TYPE_INT64,
                'nullable' => false,
            ],
            'col_string' => [
                'type' => Bigquery::TYPE_STRING,
                'nullable' => true,
            ],
            'col_bool' => [
                'type' => Bigquery::TYPE_BOOL,
                'nullable' => true,
            ],
            'col_int' => [
                'type' => Bigquery::TYPE_INT64,
                'nullable' => true,
            ],
            'col_decimal' => [
                'type' => Bigquery::TYPE_DECIMAL,
                'nullable' => true,
            ],
            'col_float' => [
                'type' => Bigquery::TYPE_FLOAT64,
                'nullable' => true,
            ],
            'col_date' => [
                'type' => Bigquery::TYPE_DATE,
                'nullable' => true,
            ],
        ],
    ];

    private string $bucketName;

    public function testCreateProfile(): void
    {
        $handler = new ProfileTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = new CreateProfileTableCommand();
        $command->setPath([$this->bucketName]);
        $command->setTableName(self::TABLE_NAME);

        $response = $handler(
            $this->projects[0][0],
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(CreateProfileTableResponse::class, $response);

        $this->assertSame('profile_table_test', $response->getTableName());
        $this->assertSame([$this->bucketName], iterator_to_array($response->getPath()));
        $this->assertSame('{"rowCount":8,"columnCount":7,"dataSize":483}', $response->getProfile());

        /** @var array<string, string> $expectedColumnProfiles */
        $expectedColumnProfiles = [
            'id' => '{"distinctCount":8,"duplicateCount":0,"nullCount":0,"numericStatistics":{"avg":4.5,"mode":8,"median":4.5,"min":1,"max":8}}', // phpcs:ignore
            'col_string' => '{"distinctCount":7,"duplicateCount":1,"nullCount":0,"length":{"avg":14.5,"min":9,"max":20}}', // phpcs:ignore
            'col_bool' => '{"distinctCount":2,"duplicateCount":5,"nullCount":1}',
            'col_int' => '{"distinctCount":5,"duplicateCount":2,"nullCount":1,"numericStatistics":{"avg":75.714286,"mode":120,"median":60,"min":0,"max":200}}', // phpcs:ignore
            'col_decimal' => '{"distinctCount":6,"duplicateCount":1,"nullCount":1,"numericStatistics":{"avg":108.838571,"mode":29.99,"median":29.99,"min":15.5,"max":499}}', // phpcs:ignore
            'col_float' => '{"distinctCount":5,"duplicateCount":2,"nullCount":1,"numericStatistics":{"avg":4.114286,"mode":4.5,"median":4.5,"min":2.4,"max":4.9}}', // phpcs:ignore
            'col_date' => '{"distinctCount":6,"duplicateCount":1,"nullCount":1}',
        ];

        /** @var Column[] $columns */
        $columns = iterator_to_array($response->getColumns());
        foreach ($columns as $column) {
            $this->assertInstanceOf(Column::class, $column);
            $this->assertArrayHasKey($column->getName(), $expectedColumnProfiles);
            $this->assertSame($expectedColumnProfiles[$column->getName()], $column->getProfile());
        }
    }

    protected function setUp(): void
    {
        parent::setUp();
        $projectCredentials = $this->projects[0][0];

        $this->bucketName = $this->createTestBucket($projectCredentials)->getCreateBucketObjectName();
        $this->createTable($projectCredentials, $this->bucketName, self::TABLE_NAME, self::TABLE_STRUCTURE);

        $bigQuery = $this->clientManager->getBigQueryClient($this->testRunId, $projectCredentials);

        $bigQuery->runQuery($bigQuery->query(sprintf(
            <<<'SQL'
                INSERT INTO `%s.%s` (id, col_string, col_bool, col_int, col_decimal, col_float, col_date) VALUES
                (1, 'Bluetooth Headphones', TRUE, 120, 29.99, 4.5, DATE '2023-03-01'),
                (2, 'Bluetooth Headphones', TRUE, 120, 29.99, 4.5, DATE '2023-03-01'),
                (3, 'Smartphone X200', FALSE, 0, 499.00, 4.5, DATE '2022-11-15'),
                (4, 'Wireless Mouse', TRUE, NULL, 15.50, 3.9, DATE '2024-01-20'),
                (5, 'Mechanical Keyboard', TRUE, 200, 89.90, 4.1, NULL),
                (6, '4K OLED TV', NULL, 0, NULL, 4.9, DATE '2020-09-05'),
                (7, 'USB-C Hub', TRUE, 30, 22.49, NULL, DATE '2023-12-12'),
                (8, 'Ultrabook', FALSE, 60, 75.00, 2.4, DATE '2021-05-25')
                SQL,
            $this->bucketName,
            self::TABLE_NAME,
        )));
    }
}

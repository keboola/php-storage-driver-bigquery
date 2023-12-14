<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Table\Create\Helper;

use Generator;
use Google\Protobuf\Any;
use Keboola\StorageDriver\Backend\BigQuery\Clustering;
use Keboola\StorageDriver\Backend\BigQuery\RangePartitioning;
use Keboola\StorageDriver\Backend\BigQuery\TimePartitioning;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\Helper\CreateTableMetaHelper;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use PHPUnit\Framework\TestCase;

class CreateTableMetaHelperTest extends TestCase
{
    public function caseGenerator(): Generator
    {
        yield 'no metadata' => [
            (new CreateTableCommand()),
            [],
        ];
        $meta = new Any();
        $meta->pack((new CreateTableCommand\BigQueryTableMeta()));
        yield 'empty metadata' => [
            (new CreateTableCommand())->setMeta($meta),
            [],
        ];
        $meta = new Any();
        $meta->pack((new CreateTableCommand\BigQueryTableMeta())->setRequirePartitionFilter(true));
        yield 'partition filter without partitioning' => [
            (new CreateTableCommand())->setMeta($meta),
            [],
        ];
        $meta = new Any();
        $meta->pack((new CreateTableCommand\BigQueryTableMeta())->setTimePartitioning(
            (new TimePartitioning())
                ->setType('DAY')
                ->setField('date')
                ->setExpirationMs('1000'),
        ));
        yield 'time partitioning' => [
            (new CreateTableCommand())->setMeta($meta),
            [
                'timePartitioning' => [
                    'type' => 'DAY',
                    'expirationMs' => 1000,
                    'field' => 'date',
                ],
                'requirePartitionFilter' => false,
            ],
        ];

        $meta = new Any();
        $meta->pack((new CreateTableCommand\BigQueryTableMeta())->setTimePartitioning(
            (new TimePartitioning())
                ->setType('DAY'),
        ));
        yield 'time partitioning minimal' => [
            (new CreateTableCommand())->setMeta($meta),
            [
                'timePartitioning' => [
                    'type' => 'DAY',
                ],
                'requirePartitionFilter' => false,
            ],
        ];

        $meta = new Any();
        $meta->pack((new CreateTableCommand\BigQueryTableMeta())->setTimePartitioning(
            (new TimePartitioning())
                ->setType('DAY')
                ->setField('date')
                ->setExpirationMs('1000'),
        )->setClustering((new Clustering())->setFields(['col1', 'col2'])));
        yield 'time partitioning with clustering' => [
            (new CreateTableCommand())->setMeta($meta),
            [
                'timePartitioning' => [
                    'type' => 'DAY',
                    'expirationMs' => 1000,
                    'field' => 'date',
                ],
                'clustering' => [
                    'fields' => ['col1', 'col2'],
                ],
                'requirePartitionFilter' => false,
            ],
        ];
        $meta = new Any();
        $meta->pack((new CreateTableCommand\BigQueryTableMeta()
        )->setTimePartitioning(
            (new TimePartitioning())
                ->setType('DAY')
                ->setField('date')
                ->setExpirationMs('1000'),
        )->setClustering((new Clustering())->setFields(['col1', 'col2']))->setRangePartitioning(
            (new RangePartitioning())
                ->setField('col1')
                ->setRange(
                    (new RangePartitioning\Range())
                        ->setStart('1')
                        ->setEnd('100')
                        ->setInterval('10'),
                ),
        )->setRequirePartitionFilter(true));
        yield 'time partitioning with clustering with range' => [
            (new CreateTableCommand())->setMeta($meta),
            [
                'timePartitioning' => [
                    'type' => 'DAY',
                    'expirationMs' => 1000,
                    'field' => 'date',
                ],
                'clustering' => [
                    'fields' => ['col1', 'col2'],
                ],
                'rangePartitioning' => [
                    'field' => 'col1',
                    'range' => [
                        'start' => 1,
                        'end' => 100,
                        'interval' => 10,
                    ],
                ],
                'requirePartitionFilter' => true,
            ],
        ];
    }

    /**
     * @dataProvider caseGenerator
     * @param array<mixed> $expectedOptions
     */
    public function test(CreateTableCommand $cmd, array $expectedOptions): void
    {
        $output = CreateTableMetaHelper::convertTableMetaToRest($cmd);
        $this->assertSame($expectedOptions, $output);
    }

    public function testWrongMetaObject(): void
    {
        $cmd = new CreateTableCommand();
        $meta = new Any();
        $meta->pack((new TableInfo\BigQueryTableMeta()));
        $cmd->setMeta($meta);

        $this->expectException(Exception::class);
        CreateTableMetaHelper::convertTableMetaToRest($cmd);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Create\Helper;

use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Shared\Utils\MetaHelper;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;

final class CreateTableMetaHelper
{
    /**
     * @return array{
     *     timePartitioning?: array{
     *      type: string,
     *      expirationMs?: int,
     *      field?: string
     *     },
     *     clustering?: array{
     *      fields: string[]
     *     },
     *     rangePartitioning?: array{
     *      field: string,
     *      range: array{
     *          start: int,
     *          end: int,
     *          interval: int
     *      }
     *     }
     * }
     * @throws Exception
     */
    public static function convertTableMetaToRest(CreateTableCommand $command): array
    {
        $meta = MetaHelper::getMetaFromCommand(
            $command,
            CreateTableCommand\BigQueryTableMeta::class
        );
        if ($meta === null) {
            return [];
        }

        $options = [];

        assert($meta instanceof CreateTableCommand\BigQueryTableMeta);
        if ($meta->getTimePartitioning() !== null) {
            $timePartitioningOptions = [
                'type' => $meta->getTimePartitioning()->getType(),
            ];
            if ($meta->getTimePartitioning()->getExpirationMs() !== null) {
                $timePartitioningOptions['expirationMs'] = (int) $meta->getTimePartitioning()->getExpirationMs();
            }
            if ($meta->getTimePartitioning()->getField() !== null) {
                $timePartitioningOptions['field'] = $meta->getTimePartitioning()->getField();
            }
            $options['timePartitioning'] = $timePartitioningOptions;
        }
        if ($meta->getClustering() !== null) {
            $options['clustering'] = [
                'fields' => ProtobufHelper::repeatedStringToArray($meta->getClustering()->getFields()),
            ];
        }
        if ($meta->getRangePartitioning() !== null) {
            assert($meta->getRangePartitioning()->getRange() !== null);
            $options['rangePartitioning'] = [
                'field' => $meta->getRangePartitioning()->getField(),
                'range' => [
                    'start' => (int) $meta->getRangePartitioning()->getRange()->getStart(),
                    'end' => (int) $meta->getRangePartitioning()->getRange()->getEnd(),
                    'interval' => (int) $meta->getRangePartitioning()->getRange()->getInterval(),
                ],
            ];
        }
        $options['requirePartitionFilter'] = $meta->getRequirePartitionFilter();

        return $options;
    }
}

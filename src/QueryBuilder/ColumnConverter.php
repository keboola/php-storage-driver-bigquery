<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

class ColumnConverter
{
    public const DATA_TYPES_OPTIONS = [
        DataType::INTEGER,
        DataType::REAL,
    ];

    public const DATA_TYPES_MAP = [
        DataType::STRING => Bigquery::TYPE_STRING,
        DataType::INTEGER => Bigquery::TYPE_INT64,
        DataType::DOUBLE => Bigquery::TYPE_NUMERIC,
        DataType::BIGINT => Bigquery::TYPE_BIGINT,
        DataType::REAL => Bigquery::TYPE_NUMERIC,
        DataType::DECIMAL => Bigquery::TYPE_DECIMAL,
    ];

    /**
     * Only cast STRING type to a given NUMERIC type
     */
    public function convertColumnByDataType(string $column, int $dataType): string
    {
        if (!in_array($dataType, self::DATA_TYPES_OPTIONS, true)) {
            throw new QueryBuilderException(
                sprintf(
                    'Data type %s not recognized. Possible datatypes are [%s]',
                    self::DATA_TYPES_MAP[$dataType],
                    implode('|', array_map(
                        static fn (int $type) => self::DATA_TYPES_MAP[$type],
                        self::DATA_TYPES_OPTIONS,
                    ))
                ),
            );
        }
        if ($dataType === DataType::INTEGER) {
            return sprintf(
                'SAFE_CAST(%s AS INTEGER)',
                BigqueryQuote::quoteSingleIdentifier($column),
            );
        }
        return sprintf(
            'SAFE_CAST(%s AS NUMERIC)',
            BigqueryQuote::quoteSingleIdentifier($column),
        );
    }
}

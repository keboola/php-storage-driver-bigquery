<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

class ColumnConverter
{
    public const DATA_TYPES_OPTIONS = [
        DataType::STRING,
        DataType::INTEGER,
        DataType::DOUBLE,
        DataType::BIGINT,
        DataType::REAL,
        DataType::DECIMAL,
    ];
    public const DATA_TYPES_MAP = [
        DataType::STRING => Bigquery::TYPE_STRING,
        DataType::INTEGER => Bigquery::TYPE_INTEGER,
        DataType::DOUBLE => Bigquery::TYPE_NUMERIC,
        DataType::BIGINT => Bigquery::TYPE_BIGINT,
        DataType::REAL => Bigquery::TYPE_NUMERIC,
        DataType::DECIMAL => Bigquery::TYPE_DECIMAL,
    ];

    /**
     * Only cast STRING type to a given NUMERIC type
     */
    public function convertColumnByDataType(string $tableName, string $column, int $dataType): string
    {
        if (!in_array($dataType, self::DATA_TYPES_OPTIONS, true)) {
            throw new QueryBuilderException(
                sprintf(
                    'Data type %s not recognized. Possible datatypes are [%s]',
                    DataType::name($dataType),
                    implode('|', array_map(
                        static fn(int $type) => self::DATA_TYPES_MAP[$type],
                        self::DATA_TYPES_OPTIONS,
                    ))
                ),
            );
        }
        return sprintf(
            'SAFE_CAST(%s.%s AS %s)',
            BigqueryQuote::quoteSingleIdentifier($tableName),
            BigqueryQuote::quoteSingleIdentifier($column),
            self::DATA_TYPES_MAP[$dataType]
        );
    }
}

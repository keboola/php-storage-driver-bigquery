<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Helpers;

use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;

final class CreateImportOptionHelper
{
    public static function createOptions(
        ImportOptions $options
    ): BigqueryImportOptions {
        $strategyMapping = [
            ImportStrategy::STRING_TABLE => ImportOptionsInterface::USING_TYPES_STRING,
            ImportStrategy::USER_DEFINED_TABLE => ImportOptionsInterface::USING_TYPES_USER,
        ];
        return new BigqueryImportOptions(
            ProtobufHelper::repeatedStringToArray($options->getConvertEmptyValuesToNullOnColumns()),
            $options->getImportType() === ImportType::INCREMENTAL,
            $options->getTimestampColumn() === '_timestamp',
            $options->getNumberOfIgnoredLines(),
            $strategyMapping[$options->getImportStrategy()]
        );
    }
}

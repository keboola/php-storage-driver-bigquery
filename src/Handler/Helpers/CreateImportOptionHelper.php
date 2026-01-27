<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Helpers;

use InvalidArgumentException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\TimestampMode;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;

final class CreateImportOptionHelper
{
    /**
     * @param string[] $features
     */
    public static function createOptions(
        ImportOptions $options,
        array $features,
    ): BigqueryImportOptions {
        $usingTypes = match ($options->getImportStrategy()) {
            ImportStrategy::STRING_TABLE => ImportOptionsInterface::USING_TYPES_STRING,
            ImportStrategy::USER_DEFINED_TABLE => ImportOptionsInterface::USING_TYPES_USER,
            default => throw new InvalidArgumentException('Unknown import strategy ' . $options->getImportStrategy()),
        };
        $timestampMode = match ($options->getTimestampMode()) {
            ImportOptions\TimestampMode::CURRENT_TIME => TimestampMode::CurrentTime,
            ImportOptions\TimestampMode::FROM_SOURCE => TimestampMode::FromSource,
            ImportOptions\TimestampMode::NONE => TimestampMode::None,
            default => throw new InvalidArgumentException('Unknown timestamp mode ' . $options->getTimestampMode()),
        };
        return new BigqueryImportOptions(
            ProtobufHelper::repeatedStringToArray($options->getConvertEmptyValuesToNullOnColumns()),
            $options->getImportType() === ImportType::INCREMENTAL,
            $options->getTimestampColumn() === '_timestamp',
            $options->getNumberOfIgnoredLines(),
            $usingTypes,
            null,
            ProtobufHelper::repeatedStringToArray($options->getImportAsNull()),
            $features,
            $timestampMode,
        );
    }
}

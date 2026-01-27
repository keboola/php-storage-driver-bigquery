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
            ImportStrategy::USER_DEFINED_TABLE => ImportOptionsInterface::USING_TYPES_USER,
            default => ImportOptionsInterface::USING_TYPES_STRING,
        };
        $timestampMode = match ($options->getTimestampMode()) {
            ImportOptions\TimestampMode::FROM_SOURCE => TimestampMode::FromSource,
            ImportOptions\TimestampMode::NONE => TimestampMode::None,
            default => TimestampMode::CurrentTime,
        };
        $useTimestamp = $options->getTimestampColumn() === '_timestamp';
        if ($timestampMode === TimestampMode::FromSource || $timestampMode === TimestampMode::None) {
            $useTimestamp = false;
        }
        return new BigqueryImportOptions(
            ProtobufHelper::repeatedStringToArray($options->getConvertEmptyValuesToNullOnColumns()),
            $options->getImportType() === ImportType::INCREMENTAL,
            $useTimestamp,
            $options->getNumberOfIgnoredLines(),
            $usingTypes,
            null,
            ProtobufHelper::repeatedStringToArray($options->getImportAsNull()),
            $features,
            $timestampMode,
        );
    }
}

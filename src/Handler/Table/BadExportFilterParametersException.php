<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table;

use Google\Cloud\Core\Exception\BadRequestException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class BadExportFilterParametersException extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(string $message, int $code = self::ERR_VALIDATION, ?Throwable $previous = null)
    {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }

    /**
     * @throws self
     */
    public static function handleWrongTypeInFilters(BigqueryException|BadRequestException $e): void
    {
        if (str_contains($e->getMessage(), 'No matching signature for operator ')) {
            $expectedActualPattern = '/types:\s(.*?)\./';
            preg_match($expectedActualPattern, $e->getMessage(), $matches);
            assert(isset($matches[1]));
            $expected = trim(explode(',', $matches[1])[0]);
            $actual = trim(explode(',', $matches[1])[1]);

            throw new self(
                message: sprintf('Invalid filter value, expected:"%s", actual:"%s".', $expected, $actual),
                previous: $e,
            );
        }

        if (str_contains($e->getMessage(), 'Invalid')) {
            throw new self(
                message: DecodeErrorMessage::getErrorMessage($e),
                previous: $e,
            );
        }

        if (str_contains($e->getMessage(), 'can be used for partition elimination')) {
            //{
            //  "error": {
            //    "code": 400,
            //    "message": "Cannot query over table 'tableName' without a filter over column(s) 'columnName'
            // that can be used for partition elimination",
            //    "errors": [
            //      {
            //        "message": "Cannot query over table 'tableName' without a filter over column(s) 'columnName'
            // that can be used for partition elimination",
            //        "domain": "global",
            //        "reason": "invalidQuery",
            //        "location": "q",
            //        "locationType": "parameter"
            //      }
            //    ],
            //    "status": "INVALID_ARGUMENT"
            //  }
            //}
            throw new self(
                message: DecodeErrorMessage::getErrorMessage($e),
                previous: $e,
            );
        }
    }

    public static function createUnsupportedDatatypeInWhereFilter(string $columnName, string $columnType): self
    {
        return new self(
            sprintf(
                'Filtering by column "%s" of type "%s" is not supported by the backend "bigquery".',
                $columnName,
                $columnType,
            ),
        );
    }
}

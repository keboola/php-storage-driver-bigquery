<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Alter;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Table\AddPrimaryKeyCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class AddPrimaryKeyHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param AddPrimaryKeyCommand $command
     * @param string[] $features
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof AddPrimaryKeyCommand);

        // validate
        assert($command->getPath()->count() === 1, 'AddPrimaryKeyCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'AddPrimaryKeyCommand.tableName is required');
        assert(
            $command->getPrimaryKeysNames()->count() >= 1,
            'AddPrimaryKeyCommand.primaryKeysNames is required and cannot be empty',
        );

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );

        /** @var string $databaseName */
        $databaseName = $command->getPath()[0];

        // detect duplicities
        $desiredPks = ProtobufHelper::repeatedStringToArray($command->getPrimaryKeysNames());
        $formattedColumns = implode(
            ',',
            array_map(
                fn($item) => BigqueryQuote::quoteSingleIdentifier($item),
                $desiredPks,
            ),
        );
        $sqlCommand = sprintf(
/** @lang BigQuery */<<<SQL
SELECT MAX(`_row_number_`) AS `max` FROM
(
    SELECT ROW_NUMBER() OVER (PARTITION BY %s) AS `_row_number_` FROM %s.%s
) `data`
SQL,
            $formattedColumns,
            BigqueryQuote::quoteSingleIdentifier($databaseName),
            BigqueryQuote::quoteSingleIdentifier($command->getTableName()),
        );

        $result = iterator_to_array($bqClient->runQuery($bqClient->query($sqlCommand)));
        assert(count($result) === 1, 'Query to check duplicates is expected to return exactly one row');
        assert(is_array($result[0]), 'Expected array result');
        assert(array_key_exists('max', $result[0]), 'Expected "max" key in result');
        if ($result[0]['max'] > 1) {
            throw CannotAddPrimaryKeyException::createForDuplicates();
        }

        // check if table has PK set
        // pks must not be set
        $reflection = new BigqueryTableReflection($bqClient, $databaseName, $command->getTableName());
        if ($reflection->getPrimaryKeysNames() !== []) {
            throw CannotAddPrimaryKeyException::createForExistingPK();
        }

        /** @var BigqueryColumn $columnDefinition */
        foreach ($reflection->getColumnsDefinitions() as $columnDefinition) {
            $isPk = in_array($columnDefinition->getColumnName(), $desiredPks, true);
            if ($isPk && $columnDefinition->getColumnDefinition()->isNullable()) {
                throw CannotAddPrimaryKeyException::createForNullableColumn($columnDefinition->getColumnName());
            }
        }

        // add PK
        $bqClient->dataset($databaseName)->table($command->getTableName())->update(
            [
                'tableConstraints' => [
                    'primaryKey' => [
                        'columns' => $desiredPks,
                    ],
                ],
            ],
        );

        return null;
    }
}

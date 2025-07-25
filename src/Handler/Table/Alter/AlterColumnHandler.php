<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Alter;

use Google\Cloud\BigQuery\Exception\JobException;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\BigQuery\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\AlterColumnCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

final class AlterColumnHandler extends BaseHandler
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
     * @param AlterColumnCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof AlterColumnCommand);

        assert($runtimeOptions->getMeta() === null);

        $columnDefinition = $command->getDesiredDefiniton();
        // validate
        assert($command->getPath()->count() === 1, 'AlterColumnCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'AlterColumnCommand.tableName is required');
        assert($columnDefinition instanceof TableColumnShared, 'AlterColumnCommand.desiredDefinition is required');

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            iterator_to_array($runtimeOptions->getQueryTags()),
        );

        // define columns
        // validate
        assert($columnDefinition->getName() !== '', 'TableColumnShared.name is required');
        assert($columnDefinition->getType() !== '', 'TableColumnShared.type is required');

        $bqColumn =
            new Bigquery($columnDefinition->getType(), [
                'length' => $columnDefinition->getLength(),
                'nullable' => $columnDefinition->getNullable(),
                'default' => $columnDefinition->getDefault(),
            ]);

        // build sql
        $builder = new BigqueryTableQueryBuilder();
        /** @var string $databaseName */
        $databaseName = $command->getPath()[0];

        try {
            /** @var string[] $metadataKeysToUpdate */
            $metadataKeysToUpdate = iterator_to_array($command->getAttributesToUpdate()->getIterator());
            $alterColumnCommands = $builder->getUpdateColumnFromDefinitionQuery(
                $bqColumn,
                $databaseName,
                $command->getTableName(),
                $columnDefinition->getName(),
                $metadataKeysToUpdate,
            );
        } catch (QueryBuilderException $e) {
            throw new AlterColumnException(
                message: $e->getMessage(),
                previous: $e,
            );
        }

        foreach ($alterColumnCommands as $operation => $sqlCommand) {
            try {
                $bqClient->runQuery($bqClient->query($sqlCommand));
                // logging info to add it to error message as partial success of the job
                $this->userLogger->info($operation);
            } catch (JobException|BadRequestException $e) {
                // warnings will be handled then in connection for user error
                $this->userLogger->error(sprintf('"%s": %s', $operation, DecodeErrorMessage::getErrorMessage($e)));
            }
        }

        // at the end, it has to return objectInfo anyway, because connection has to update metadata
        return (new ObjectInfoResponse())
            ->setPath($command->getPath())
            ->setObjectType(ObjectType::TABLE)
            ->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $databaseName,
                new BigqueryTableReflection(
                    $bqClient,
                    $databaseName,
                    $command->getTableName(),
                ),
            ));
    }
}

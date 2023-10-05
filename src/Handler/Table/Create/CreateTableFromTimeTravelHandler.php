<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Create;

use Google\Cloud\Core\Exception\NotFoundException;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\CreateTableFromTimeTravelCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\ObjectNotFoundException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class CreateTableFromTimeTravelHandler implements DriverCommandHandlerInterface
{
    public const TIME_TRAVEL_TIMESTAMP_FORMAT = 'Y-m-d H:i:s.u e';
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateTableFromTimeTravelCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateTableFromTimeTravelCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null, 'CreateTableFromTimeTravelCommand.source is required.');
        $destination = $command->getDestination();
        assert($destination !== null, 'CreateTableFromTimeTravelCommand.destination is required.');
        $timestamp = $command->getTimestamp();
        assert($timestamp !== null, 'CreateTableFromTimeTravelCommand.timestamp is required.');
        assert(
            $sourceMapping->getPath()->count() === 1,
            'CreateTableFromTimeTravelCommand.source.path is required and size must equal 1'
        );
        /** @var string $sourceDatasetName */
        $sourceDatasetName = $sourceMapping->getPath()[0];

        assert(
            $destination->getPath()->count() === 1,
            'CreateTableFromTimeTravelCommand.destination.path is required and size must equal 1'
        );
        /** @var string $destinationDatasetName */
        $destinationDatasetName = $destination->getPath()[0];

        /** @var string $sourceTableName */
        $sourceTableName = $sourceMapping->getTableName();
        /** @var string $destinationTableName */
        $destinationTableName = $destination->getTableName();

        /** @var string $datetime */
        $datetime = date(self::TIME_TRAVEL_TIMESTAMP_FORMAT, (int) $timestamp);
        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        $query = sprintf(
            'CREATE TABLE %s.%s AS SELECT * FROM %s.%s FOR SYSTEM_TIME AS OF %s;',
            BigqueryQuote::quoteSingleIdentifier($destinationDatasetName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
            BigqueryQuote::quoteSingleIdentifier($sourceDatasetName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quote($datetime)
        );

        try {
            $bqClient->runQuery($bqClient->query($query));
        } catch (NotFoundException $e) {
            throw new ObjectNotFoundException($sourceTableName);
        }

        $destinationRef = new BigqueryTableReflection(
            $bqClient,
            ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
            $destination->getTableName()
        );

        $destinationStats = $destinationRef->getTableStats();

        $tableInfo = TableReflectionResponseTransformer::transformTableReflectionToResponse(
            $destinationDatasetName,
            new BigqueryTableReflection(
                $bqClient,
                $destinationDatasetName,
                $destinationTableName
            )
        );

        $tableInfo->setRowsCount($destinationStats->getRowsCount());
        $tableInfo->setSizeBytes($destinationStats->getDataSizeBytes());
        return (new ObjectInfoResponse())
            ->setPath($destination->getPath())
            ->setObjectType(ObjectType::TABLE)
            ->setTableInfo($tableInfo);
    }
}

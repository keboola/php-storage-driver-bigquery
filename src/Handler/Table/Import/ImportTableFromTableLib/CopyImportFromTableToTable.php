<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableLib;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Exception\JobException;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\Db\ImportExport\Backend\Assert;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

class CopyImportFromTableToTable implements ToStageImporterInterface
{
    private const TIMER_TABLE_IMPORT = 'copyToStaging';

    private BigQueryClient $bqClient;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
    }

    public function importToStagingTable(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destinationDefinition,
        ImportOptionsInterface $options,
    ): ImportState {
        assert($destinationDefinition instanceof BigqueryTableDefinition);
        assert($options instanceof BigqueryImportOptions);
        assert($source instanceof Table, sprintf(
            'Source must be instance of "%s".',
            Table::class,
        ));
        $state = new ImportState($destinationDefinition->getTableName());

        $state->startTimer(self::TIMER_TABLE_IMPORT);
        try {
            if ($options->usingUserDefinedTypes()) {
                Assert::assertSameColumnsOrdered(
                    source: (new BigqueryTableReflection(
                        $this->bqClient,
                        $source->getSchema(),
                        $source->getTableName(),
                    ))->getColumnsDefinitions(),
                    destination: $destinationDefinition->getColumnsDefinitions(),
                    assertOptions: Assert::ASSERT_MINIMAL,
                );
            }

            $sql = sprintf(
                'CREATE TABLE %s.%s COPY %s.%s',
                BigqueryQuote::quoteSingleIdentifier($destinationDefinition->getSchemaName()),
                BigqueryQuote::quoteSingleIdentifier($destinationDefinition->getTableName()),
                BigqueryQuote::quoteSingleIdentifier($source->getSchema()),
                BigqueryQuote::quoteSingleIdentifier($source->getTableName()),
            );

            $this->bqClient->runQuery($this->bqClient->query($sql));

            $ref = new BigqueryTableReflection(
                $this->bqClient,
                $destinationDefinition->getSchemaName(),
                $destinationDefinition->getTableName(),
            );

            $state->addImportedRowsCount($ref->getRowsCount());
        } catch (JobException|ServiceException $e) {
            throw BigqueryException::covertException($e);
        }
        $state->stopTimer(self::TIMER_TABLE_IMPORT);

        return $state;
    }
}

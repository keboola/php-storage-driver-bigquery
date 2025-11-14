<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table as CommandDestination;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;

/**
 * Immutable context for import operations.
 *
 * This parameter object consolidates all parameters needed for import execution,
 * reducing method signatures from 7-8 parameters to a single context object.
 */
final readonly class ImportContext
{
    /**
     * @param BigQueryClient $bqClient BigQuery client instance
     * @param CommandDestination $destination Destination table configuration
     * @param BigqueryTableDefinition $destinationDefinition Destination table definition
     * @param ImportOptions $importOptions Import operation options
     * @param SqlSourceInterface $source Source object (Table or SelectSource)
     * @param BigqueryTableDefinition $sourceTableDefinition Source table definition
     * @param BigqueryImportOptions $bigqueryImportOptions BigQuery-specific import options
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping configuration
     */
    public function __construct(
        public BigQueryClient $bqClient,
        public CommandDestination $destination,
        public BigqueryTableDefinition $destinationDefinition,
        public ImportOptions $importOptions,
        public SqlSourceInterface $source,
        public BigqueryTableDefinition $sourceTableDefinition,
        public BigqueryImportOptions $bigqueryImportOptions,
        public TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ) {
    }

    /**
     * Creates import context from individual parameters.
     *
     * This static factory method provides a clear, named way to construct
     * the context object.
     *
     * @param BigQueryClient $bqClient BigQuery client
     * @param CommandDestination $destination Destination configuration
     * @param BigqueryTableDefinition $destinationDefinition Destination definition
     * @param ImportOptions $importOptions Import options
     * @param SqlSourceInterface $source Source object
     * @param BigqueryTableDefinition $sourceTableDefinition Source definition
     * @param BigqueryImportOptions $bigqueryImportOptions BigQuery options
     * @param TableImportFromTableCommand\SourceTableMapping $sourceMapping Source mapping
     * @return self The constructed context
     */
    public static function create(
        BigQueryClient $bqClient,
        CommandDestination $destination,
        BigqueryTableDefinition $destinationDefinition,
        ImportOptions $importOptions,
        SqlSourceInterface $source,
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryImportOptions $bigqueryImportOptions,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): self {
        return new self(
            $bqClient,
            $destination,
            $destinationDefinition,
            $importOptions,
            $source,
            $sourceTableDefinition,
            $bigqueryImportOptions,
            $sourceMapping,
        );
    }
}

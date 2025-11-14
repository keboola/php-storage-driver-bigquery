<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;

/**
 * Immutable context object containing source information for import operations.
 *
 * This value object encapsulates all information about the import source,
 * including the source object itself and related table definitions.
 */
final readonly class SourceContext
{
    /**
     * @param SqlSourceInterface $source The source object (Table or SelectSource)
     * @param BigqueryTableDefinition $effectiveDefinition Filtered definition with only selected columns
     * @param BigqueryTableDefinition $fullDefinition Complete source table definition with all columns
     * @param string[] $selectedColumns List of column names that were selected for import
     */
    public function __construct(
        public SqlSourceInterface $source,
        public BigqueryTableDefinition $effectiveDefinition,
        public BigqueryTableDefinition $fullDefinition,
        public array $selectedColumns,
    ) {}
}

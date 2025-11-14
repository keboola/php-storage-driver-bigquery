<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table as CommandDestination;

/**
 * Request object for VIEW creation operations.
 *
 * This parameter object consolidates parameters needed for creating a BigQuery VIEW
 * that references a source table.
 */
final readonly class ViewCreationRequest
{
    /**
     * @param CommandDestination $destination Destination view configuration
     * @param Table $source Source table to be referenced by the view
     */
    public function __construct(
        public CommandDestination $destination,
        public Table $source,
    ) {}
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table as CommandDestination;

/**
 * Request object for table CLONE operations.
 *
 * This parameter object consolidates parameters needed for cloning a BigQuery table
 * using the CLONE statement or fallback to CREATE TABLE AS SELECT.
 */
final readonly class CloneRequest
{
    /**
     * @param CommandDestination $destination Destination table configuration
     * @param Table $source Source table to be cloned
     */
    public function __construct(
        public CommandDestination $destination,
        public Table $source,
    ) {
    }
}

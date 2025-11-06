<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Keboola\Db\ImportExport\Storage\Bigquery\SelectSource;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class MappedTableSqlSource extends SelectSource
{
    /** @var array<int, array{source: string, destination: string}> */
    private array $columnMappings;

    /**
     * @param array<int, array{source: string, destination: string}> $columnMappings
     * @param string[]|null $primaryKeysNames
     */
    public function __construct(
        private readonly string $schema,
        private readonly string $tableName,
        array $columnMappings,
        ?array $primaryKeysNames = null,
    ) {
        $this->columnMappings = $columnMappings;

        parent::__construct(
            '',
            [],
            array_map(static fn(array $mapping) => $mapping['destination'], $columnMappings),
            [],
            $primaryKeysNames,
        );
    }

    public function getFromStatement(): string
    {
        if ($this->columnMappings === []) {
            return sprintf(
                'SELECT * FROM %s.%s',
                BigqueryQuote::quoteSingleIdentifier($this->schema),
                BigqueryQuote::quoteSingleIdentifier($this->tableName),
            );
        }

        return $this->buildSelect(false);
    }

    public function getFromStatementWithStringCasting(): string
    {
        if ($this->columnMappings === []) {
            return $this->getFromStatement();
        }

        return $this->buildSelect(true);
    }

    private function buildSelect(bool $castToString): string
    {
        $quotedSchema = BigqueryQuote::quoteSingleIdentifier($this->schema);
        $quotedTable = BigqueryQuote::quoteSingleIdentifier($this->tableName);
        $selectParts = [];

        foreach ($this->columnMappings as $mapping) {
            $sourceColumn = sprintf(
                '%s.%s',
                $quotedTable,
                BigqueryQuote::quoteSingleIdentifier($mapping['source']),
            );
            if ($castToString) {
                $sourceColumn = sprintf('CAST(%s AS STRING)', $sourceColumn);
            }

            $selectParts[] = sprintf(
                '%s AS %s',
                $sourceColumn,
                BigqueryQuote::quoteSingleIdentifier($mapping['destination']),
            );
        }

        return sprintf(
            'SELECT %s FROM %s.%s',
            implode(', ', $selectParts),
            $quotedSchema,
            $quotedTable,
        );
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Keboola\Db\ImportExport\Storage\Bigquery\SelectSource;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use LogicException;

final class MappedTableSqlSource extends SelectSource
{
    private const SUBQUERY_ALIAS = '_mapped_source';

    /** @var array<int, array{source: string, destination: string}> */
    private array $columnMappings;

    /**
     * @param array<int, array{source: string, destination: string}> $columnMappings
     * @param string[]|null $primaryKeysNames
     * @param array<int|string, mixed> $queryBindings
     */
    public function __construct(
        private readonly ?string $schema,
        private readonly ?string $tableName,
        array $columnMappings,
        ?array $primaryKeysNames = null,
        private readonly ?string $baseQuery = null,
        array $queryBindings = [],
    ) {
        if ($queryBindings === []) {
            /** @var array<string, mixed> $queryBindings */
            $queryBindings = [];
        } elseif (array_is_list($queryBindings)) {
            throw new LogicException('Query bindings must use named parameters.');
        } else {
            foreach (array_keys($queryBindings) as $bindingKey) {
                if (!is_string($bindingKey)) {
                    throw new LogicException('Query bindings must use named parameters.');
                }
            }
            /** @var array<string, mixed> $queryBindings */
        }

        if ($baseQuery === null && ($schema === null || $tableName === null)) {
            throw new LogicException('Either base query or schema and tableName must be provided.');
        }
        $this->columnMappings = $columnMappings;

        parent::__construct(
            '',
            $queryBindings,
            array_map(static fn(array $mapping) => $mapping['destination'], $columnMappings),
            [],
            $primaryKeysNames,
        );
    }

    public function getFromStatement(): string
    {
        if ($this->columnMappings === []) {
            return $this->baseQuery ?? sprintf(
                'SELECT * FROM %s.%s',
                BigqueryQuote::quoteSingleIdentifier((string) $this->schema),
                BigqueryQuote::quoteSingleIdentifier((string) $this->tableName),
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
        $selectParts = [];
        $sourceAlias = $this->getSourceAlias();

        foreach ($this->columnMappings as $mapping) {
            $sourceColumn = sprintf(
                '%s.%s',
                $sourceAlias,
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
            'SELECT %s FROM %s',
            implode(', ', $selectParts),
            $this->buildFromClause(),
        );
    }

    private function buildFromClause(): string
    {
        if ($this->baseQuery !== null) {
            return sprintf(
                '(%s) AS %s',
                $this->baseQuery,
                BigqueryQuote::quoteSingleIdentifier(self::SUBQUERY_ALIAS),
            );
        }

        return sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier((string) $this->schema),
            BigqueryQuote::quoteSingleIdentifier((string) $this->tableName),
        );
    }

    private function getSourceAlias(): string
    {
        if ($this->baseQuery !== null) {
            return BigqueryQuote::quoteSingleIdentifier(self::SUBQUERY_ALIAS);
        }

        if ($this->tableName === null) {
            throw new LogicException('Table name must be provided when base query is not used.');
        }

        return BigqueryQuote::quoteSingleIdentifier($this->tableName);
    }
}

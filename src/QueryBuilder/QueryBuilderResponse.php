<?php

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

class QueryBuilderResponse
{
    private string $query;
    /**
     * @var list<mixed>|array<string, mixed>
     */
    private array $bindings;
    /**
     * @var string[]
     */
    private array $types;
    /**
     * @var string[]
     */
    private array $columns;

    /**
     * @param string $query
     * @param list<array>|array<string, mixed> $bindings
     * @param string[] $types
     * @param string[] $columns
     */
    public function __construct(
        string $query,
        array $bindings,
        array $types,
        array $columns
    ) {
        $this->query = $query;
        $this->bindings = $bindings;
        $this->types = $types;
        $this->columns = $columns;
    }

    /**
     * @return string
     */
    public function getQuery(): string
    {
        return $this->query;
    }

    /**
     * @return list<mixed>|array<string, mixed>
     */
    public function getBindings(): array
    {
        return $this->bindings;
    }

    /**
     * @return string[]
     */
    public function getTypes(): array
    {
        return $this->types;
    }

    /**
     * @return string[]
     */
    public function getColumns(): array
    {
        return $this->columns;
    }

}

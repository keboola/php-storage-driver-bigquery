<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

class QueryBuilderResponse
{
    private string $query;
    /**
     * @var list<mixed>|array<string, mixed>
     */
    private array $bindings;

    /**
     * @param list<array>|array<string, mixed> $bindings
     */
    public function __construct(
        string $query,
        array $bindings
    ) {
        $this->query = $query;
        $this->bindings = $bindings;
    }

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
        // this query builder does not support types
        return [];
    }
}

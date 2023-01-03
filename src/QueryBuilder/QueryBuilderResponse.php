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
     * @var string[]
     */
    private array $types;

    /**
     * @param list<array>|array<string, mixed> $bindings
     * @param string[] $types
     */
    public function __construct(
        string $query,
        array $bindings,
        array $types
    ) {
        $this->query = $query;
        $this->bindings = $bindings;
        $this->types = $types;
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
        return $this->types;
    }
}

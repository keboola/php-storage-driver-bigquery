<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\DeleteRows;

final class Table
{
    /**
     * @var string[]
     */
    public readonly array $columns;

    /**
     * @param array{
     *     columns: array<string, array<string, mixed>>,
     *     primaryKeysNames?: array<int, string>
     * } $structure
     */
    public function __construct(
        public readonly Bucket|Workspace $dataset,
        public readonly string $name,
        public readonly array $structure,
    ) {
        $this->columns = array_keys($structure['columns']);
    }
}

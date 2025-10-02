<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;

/**
 * When action has no implementation on BQ side and we do not throw exception
 * This handler can be used
 */
final class EmptyHandler extends BaseHandler
{
    /**
     * @inheritDoc
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        // no action from BQ side is needed
        return null;
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Project\Update;

use Google\ApiCore\ApiException;
use Google\ApiCore\ValidationException;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\ProjectIdTooLongException;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\ProjectWithProjectIdAlreadyExists;
use Keboola\StorageDriver\Command\Project\UpdateProjectCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

class UpdateProjectHandler extends BaseHandler
{
    public function __construct(
        private readonly GCPClientManager $clientManager,
    ) {
        parent::__construct();
    }

    /**
     * @inheritDoc
     * @throws ValidationException
     * @throws Exception
     * @throws ApiException
     * @throws ProjectWithProjectIdAlreadyExists
     * @throws ProjectIdTooLongException
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof UpdateProjectCommand);

        $bqClient = $this->clientManager->getBigQueryClient('', $credentials);
        $query = $bqClient->query(sprintf(
            'ALTER PROJECT %s SET OPTIONS (`region-%s.default_time_zone` = %s);',
            BigqueryQuote::quoteSingleIdentifier($command->getProjectId()),
            strtolower($command->getRegion()),
            BigqueryQuote::quoteSingleIdentifier($command->getTimezone()),
        ));
        $bqClient->runQuery($query);

        return null;
    }
}

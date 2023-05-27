<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Cloud\Iam\V1\Binding;
use Google\Cloud\Iam\V1\Policy;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\GrantBucketAccessToReadOnlyRoleHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;

class GrantRevokeBucketAccessToReadOnlyRoleTest extends BaseCase
{
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Project\Drop;

use Generator;
use Keboola\StorageDriver\BigQuery\Handler\Project\Drop\PolicyFilter;
use PHPUnit\Framework\TestCase;

class PolicyFilterTest extends TestCase
{

    /**
     * @param array<string, mixed> $expected
     * @dataProvider policyProvider
     */
    public function testRemoveServiceAccFromBucketPolicy(string $serviceAccToRemove, array $expected): void
    {
        $policy = [
            'kind' => 'storage#policy',
            'resourceId' => 'projects/_/buckets/rr-files-bq-driver',
            'version' => 1,
            'etag' => 'CGE=',
            'bindings' =>
                [
                    [
                        'role' => 'roles/storage.objectAdmin',
                        'members' =>
                            [
                                0 => 'serviceAccount:rb-m1-kbc-1290@360765987264.iam.gserviceaccount.com',
                                1 => 'serviceAccount:rb-m1-kbc-1291@1067394708865.iam.gserviceaccount.com',
                                2 => 'serviceAccount:rb-m1-kbc-1292@1067394708865.iam.gserviceaccount.com',
                            ],
                    ],
                ],
        ];

        $filtered = PolicyFilter::removeServiceAccFromBucketPolicy($policy, $serviceAccToRemove);

        $this->assertSame($expected, $filtered);
    }

    public function policyProvider(): Generator
    {
        yield 'remove rb-m1-kbc-1291' => [
            'rb-m1-kbc-1291@1067394708865.iam.gserviceaccount.com',
            [
                'kind' => 'storage#policy',
                'resourceId' => 'projects/_/buckets/rr-files-bq-driver',
                'version' => 1,
                'etag' => 'CGE=',
                'bindings' =>
                    [
                        [
                            'role' => 'roles/storage.objectAdmin',
                            'members' =>
                                [
                                    0 => 'serviceAccount:rb-m1-kbc-1290@360765987264.iam.gserviceaccount.com',
                                    1 => 'serviceAccount:rb-m1-kbc-1292@1067394708865.iam.gserviceaccount.com',
                                ],
                        ],
                    ],
            ],
        ];
    }
}

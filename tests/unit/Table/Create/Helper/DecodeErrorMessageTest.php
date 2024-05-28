<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Table\Create\Helper;

use Generator;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use PHPUnit\Framework\TestCase;

class DecodeErrorMessageTest extends TestCase
{
    public function errorProviderFroDirectExtraction(): Generator
    {
        yield 'simple message' => [
            "Cannot query over table 'local-main-0529060357146-5db0.KPjdieJ68wCATN4jm1Z2ufZ9l5Og1TZ05oF3Jvowkin_c_Test.externalTableWithConnectionAndPartitioning' without a filter over column(s) 'part' that can be used for partition eliminationError while reading table: local-main-0529060357146-5db0.KPjdieJ68wCATN4jm1Z2ufZ9l5Og1TZ05oF3Jvowkin_c_Test.externalTableWithInvalidPartitioning, error message: Incompatible partition schemas.  Expected schema ([part:TYPE_INT64]) has 1 columns. Observed schema ([]) has 0 columns.", // phpcs:ignore
            'Incompatible partition schemas.  Expected schema ([part:TYPE_INT64]) has 1 columns. Observed schema ([]) has 0 columns.',  // phpcs:ignore
        ];

        yield 'nothing to extract' => [
            'garbage in, garbage out',
            'garbage in, garbage out',
        ];
    }

    public function errorProvider(): Generator
    {
        yield 'simple message' => [
            'Simple error message',
            'Simple error message',
        ];

        yield 'empty message' => [
            '',
            '',
        ];

        yield 'json no error message' => [
            '{"data":"data"}',
            '{"data":"data"}',
        ];

        yield 'json no errors key, no message under error key, but string' => [
            '{"error":"my error"}',
            'my error',
        ];

        yield 'json no errors key, message under error key' => [
            '{"error":{"message":"error[message]"}}',
            'error[message]',
        ];

        yield 'json no errors key, no message under error key, but array' => [
            '{"error":["test"]}',
            '{"error":["test"]}',
        ];

        yield 'json errors key not array, no message under error key' => [
            '{"error":{"errors":"test"}}',
            '{"error":{"errors":"test"}}',
        ];

        yield 'json errors key not array, message under error key' => [
            '{"error":{"message":"error[message]","errors":"test"}}',
            'error[message]',
        ];

        yield 'json errors key not array, empty errors' => [
            '{"error":{"message":"error[message]","errors":[]}}',
            'error[message]',
        ];

        yield 'json errors one error' => [
            '{"error":{"message":"error[message]","errors":[{"message":"error.errors[0].message"}]}}',
            'error.errors[0].message',
        ];

        yield 'json errors more error' => [
            // phpcs:disable Generic.Files.LineLength
            '{"error":{"message":"error[message]","errors":[{"message":"error.errors[0].message"},{"message":"error.errors[1].message"}]}}',
            'Errors: error.errors[0].message
error.errors[1].message',
        ];

        yield 'test' => [
            '{"message": "Error msg","code": 5,"status": "NOT_FOUND","details": []}',
            'Error msg',
        ];
    }

    /**
     * @dataProvider errorProvider
     */
    public function testGetErrorMessage(string $message, string $expectedErrorMessage): void
    {
        $this->assertSame(
            $expectedErrorMessage,
            DecodeErrorMessage::getErrorMessage(new Exception($message)),
        );
    }
    /**
     * @dataProvider errorProviderFroDirectExtraction
     */
    public function testGetDirectErrorMessage(string $message, string $expectedErrorMessage): void
    {
        $this->assertSame(
            $expectedErrorMessage,
            DecodeErrorMessage::getDirectErrorMessage(new Exception($message)),
        );
    }
}

<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Helpers;

use Throwable;

final class DecodeErrorMessage
{
    public static function getErrorMessage(Throwable $e): string
    {
        try {
            $message = json_decode($e->getMessage(), true, 512, JSON_THROW_ON_ERROR);
        } catch (Throwable) {
            return $e->getMessage();
        }

        if (!is_array($message)) {
            return $e->getMessage();
        }

        if (array_key_exists('message', $message)) {
            return $message['message'];
        }

        if (!array_key_exists('error', $message)) {
            return $e->getMessage();
        }

        if (!is_array($message['error'])) {
            if (is_string($message['error'])) {
                return $message['error'];
            }
            return $e->getMessage();
        }

        if (!array_key_exists('errors', $message['error'])) {
            if (array_key_exists('message', $message['error'])) {
                return $message['error']['message'];
            }
            return $e->getMessage();
        }

        if (!is_array($message['error']['errors']) || count($message['error']['errors']) === 0) {
            if (array_key_exists('message', $message['error'])) {
                return $message['error']['message'];
            }
            return $e->getMessage();
        }

        if (count($message['error']['errors']) === 1) {
            return $message['error']['errors'][0]['message'];
        }
        $errors = [];
        foreach ($message['error']['errors'] as $error) {
            $errors[] = $error['message'];
        }

        return 'Errors: ' . implode(PHP_EOL, $errors);
    }
}

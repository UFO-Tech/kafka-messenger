<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Kafka;

enum TransportOption: string
{
    case Topic = 'topic';
    case KafkaConf = 'kafka_conf';
    case FlushTimeout = 'flush_timeout';
    case FlushRetries = 'flush_retries';
    case ReceiveTimeout = 'receive_timeout';
    case CommitAsync = 'commit_async';

    /** @return list<string> */
    public static function names(): array
    {
        return array_map(static fn (self $option): string => $option->value, self::cases());
    }
}

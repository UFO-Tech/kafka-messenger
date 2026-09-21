<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Kafka;

use RdKafka\Message;

final class MessagePosition
{
    private const TOPIC = 'topic';
    private const PARTITION = 'partition';
    private const OFFSET = 'offset';

    /** @return array<string, int|string|null> */
    public static function of(Message $message): array
    {
        return [
            self::TOPIC => $message->topic_name,
            self::PARTITION => $message->partition,
            self::OFFSET => $message->offset,
        ];
    }
}

<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Support;

use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\TopicPartition;

final class FakeConsumer extends KafkaConsumer
{
    public string $brokers = '';

    /** @var array<string, string> */
    public array $conf = [];

    /** @var list<string> */
    public array $subscribed = [];

    /** @var list<Message> */
    public array $queue = [];

    /** @var list<Message> */
    public array $committed = [];

    /** @var list<Message> */
    public array $committedAsync = [];

    /** @var list<null|list<TopicPartition>> */
    public array $assigned = [];

    public bool $closed = false;

    public function __construct()
    {
    }

    public function subscribe(array $topics): void
    {
        $this->subscribed = array_values($topics);
    }

    public function consume(int $timeout_ms): Message
    {
        return array_shift($this->queue) ?? self::timedOut();
    }

    public function close(): void
    {
        $this->closed = true;
    }

    public function assign(?array $topic_partitions = null): void
    {
        $this->assigned[] = $topic_partitions;
    }

    public function commit($message_or_offsets = null): void
    {
        $this->committed[] = $message_or_offsets;
    }

    public function commitAsync($message_or_offsets = null): void
    {
        $this->committedAsync[] = $message_or_offsets;
    }

    public static function message(string $payload, int $offset = 0): Message
    {
        $message = new FakeMessage();
        $message->err = \RD_KAFKA_RESP_ERR_NO_ERROR;
        $message->payload = $payload;
        $message->topic_name = 'events';
        $message->partition = 0;
        $message->offset = $offset;
        $message->headers = [];

        return $message;
    }

    public static function error(int $code, string $reason = ''): Message
    {
        $message = new FakeMessage();
        $message->err = $code;
        $message->reason = $reason;

        return $message;
    }

    private static function timedOut(): Message
    {
        return self::error(\RD_KAFKA_RESP_ERR__TIMED_OUT);
    }
}

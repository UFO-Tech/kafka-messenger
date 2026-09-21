<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Support;

use RdKafka\Message;
use RdKafka\Producer;
use RdKafka\ProducerTopic;

final class FakeProducer extends Producer
{
    public string $brokers = '';

    /** @var array<string, string> */
    public array $conf = [];

    /** @var callable(Message): void */
    public $onDelivery;

    public ?string $topicName = null;

    /** @var list<array{payload: string, key: ?string, headers: array<string, string>}> */
    public array $produced = [];

    public int $flushCalls = 0;
    public int $flushCode = \RD_KAFKA_RESP_ERR_NO_ERROR;

    /** @var list<int> */
    public array $flushCodes = [];

    public ?string $deliveryError = null;
    public bool $deliverySilently = false;

    public function __construct()
    {
    }

    public function newTopic(string $topic_name, $topic_conf = null): ProducerTopic
    {
        $this->topicName = $topic_name;

        return new FakeProducerTopic($this);
    }

    public function poll(int $timeout_ms): int
    {
        return 0;
    }

    public function flush(int $timeout_ms): int
    {
        ++$this->flushCalls;
        $code = [] === $this->flushCodes ? $this->flushCode : array_shift($this->flushCodes);

        if (\RD_KAFKA_RESP_ERR_NO_ERROR === $code && !$this->deliverySilently) {
            ($this->onDelivery)($this->report());
        }

        return $code;
    }

    private function report(): Message
    {
        $message = new FakeMessage();
        $message->err = null === $this->deliveryError ? \RD_KAFKA_RESP_ERR_NO_ERROR : \RD_KAFKA_RESP_ERR__FAIL;
        $message->reason = (string) $this->deliveryError;
        $message->payload = $this->produced[0]['payload'] ?? '';

        return $message;
    }
}

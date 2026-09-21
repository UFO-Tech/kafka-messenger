<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Support;

use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use Ufo\KafkaMessenger\Kafka\ClientFactory;

final class FakeClientFactory extends ClientFactory
{
    public FakeProducer $producer;
    public ?FakeConsumer $consumer = null;

    /** @var null|callable(KafkaConsumer, int, ?array<mixed>): void */
    public $onRebalance = null;

    public function __construct()
    {
        $this->producer = new FakeProducer();
    }

    public function producer(string $brokers, array $conf, callable $onDelivery): Producer
    {
        $this->producer->brokers = $brokers;
        $this->producer->conf = $conf;
        $this->producer->onDelivery = $onDelivery;

        return $this->producer;
    }

    public function consumer(string $brokers, array $conf, callable $onRebalance): KafkaConsumer
    {
        $this->consumer ??= new FakeConsumer();
        $this->consumer->brokers = $brokers;
        $this->consumer->conf = $conf;
        $this->onRebalance = $onRebalance;

        return $this->consumer;
    }
}

<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Kafka;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Producer;
use RdKafka\TopicPartition;

class ClientFactory
{
    /**
     * @param array<string, string>   $conf
     * @param callable(Message): void $onDelivery
     */
    public function producer(string $brokers, array $conf, callable $onDelivery): Producer
    {
        $settings = $this->conf($brokers, $conf);
        $settings->setDrMsgCb(static fn (Producer $producer, Message $message) => $onDelivery($message));

        return new Producer($settings);
    }

    /**
     * @param array<string, string>                                        $conf
     * @param callable(KafkaConsumer, int, list<TopicPartition>|null): void $onRebalance
     */
    public function consumer(string $brokers, array $conf, callable $onRebalance): KafkaConsumer
    {
        $settings = $this->conf($brokers, $conf);
        $settings->setRebalanceCb($onRebalance(...));

        return new KafkaConsumer($settings);
    }

    /** @param array<string, string> $conf */
    private function conf(string $brokers, array $conf): Conf
    {
        $settings = new Conf();
        $settings->set(ConfProperty::BrokerList->value, $brokers);

        foreach ($conf as $name => $value) {
            $settings->set($name, $value);
        }

        return $settings;
    }
}

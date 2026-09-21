<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Kafka;

use Psr\Log\LoggerInterface;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\TopicPartition;
use Symfony\Component\Messenger\Exception\TransportException;

final class Connection
{
    private ?Producer $producer = null;
    private ?ProducerTopic $topic = null;
    private ?KafkaConsumer $consumer = null;
    private bool $subscribed = false;
    private int $delivered = 0;
    private ?string $rejection = null;

    public function __construct(
        private readonly Options $options,
        private readonly ClientFactory $clients,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @param array<string, string> $headers
     *
     * @throws TransportException
     */
    public function publish(string $body, array $headers, ?string $key): void
    {
        $this->delivered = 0;
        $this->rejection = null;

        $this->topic()->producev(\RD_KAFKA_PARTITION_UA, 0, $body, $key, $headers);
        $this->producer()->poll(0);
        $this->awaitDelivery();

        $this->logger->debug('kafka: message sent to topic "{topic}"', ['topic' => $this->options->topic, 'key' => $key]);
    }

    /**
     * @return list<Message>
     *
     * @throws TransportException
     */
    public function consume(int $max): array
    {
        $consumer = $this->subscribedConsumer();

        $message = $this->receive($consumer, $this->options->receiveTimeout);
        if (null === $message) {
            return [];
        }

        $messages = [$message];
        while (\count($messages) < $max && null !== ($message = $this->tail($consumer))) {
            $messages[] = $message;
        }

        return $messages;
    }

    public function commit(Message $message): void
    {
        if ($this->options->commitAsync) {
            $this->consumer()->commitAsync($message);
        } else {
            $this->consumer()->commit($message);
        }

        $this->logger->debug('kafka: offset committed', MessagePosition::of($message));
    }

    public function close(): void
    {
        $this->consumer?->close();
        $this->consumer = null;
        $this->subscribed = false;

        $this->producer?->flush($this->options->flushTimeout);
        $this->producer = null;
        $this->topic = null;
    }

    /** @throws TransportException */
    private function awaitDelivery(): void
    {
        $code = \RD_KAFKA_RESP_ERR_NO_ERROR;
        for ($attempt = 0; $attempt <= $this->options->flushRetries; ++$attempt) {
            $code = $this->producer()->flush($this->options->flushTimeout);
            if (\RD_KAFKA_RESP_ERR_NO_ERROR === $code) {
                break;
            }
        }

        if (\RD_KAFKA_RESP_ERR_NO_ERROR !== $code) {
            throw new TransportException(\sprintf('kafka did not drain the producer queue of topic "%s" within %d ms', $this->options->topic, $this->options->flushTimeout), $code);
        }

        if (null !== $this->rejection) {
            throw new TransportException('kafka rejected the message: '.$this->rejection);
        }

        if ($this->delivered < 1) {
            throw new TransportException(\sprintf('kafka confirmed no delivery for topic "%s"', $this->options->topic));
        }
    }

    private function report(Message $message): void
    {
        if (\RD_KAFKA_RESP_ERR_NO_ERROR === $message->err) {
            ++$this->delivered;

            return;
        }

        $this->rejection ??= (string) $message->errstr();
    }

    private function tail(KafkaConsumer $consumer): ?Message
    {
        try {
            return $this->receive($consumer, 0);
        } catch (TransportException) {
            return null;
        }
    }

    /** @throws TransportException */
    private function receive(KafkaConsumer $consumer, int $timeoutMs): ?Message
    {
        $message = $consumer->consume($timeoutMs);

        return match ($message->err) {
            \RD_KAFKA_RESP_ERR_NO_ERROR => $message,
            \RD_KAFKA_RESP_ERR__PARTITION_EOF, \RD_KAFKA_RESP_ERR__TIMED_OUT => null,
            \RD_KAFKA_RESP_ERR__TRANSPORT => $this->hiccup($message),
            default => throw new TransportException($message->errstr(), $message->err),
        };
    }

    private function hiccup(Message $message): null
    {
        $this->logger->warning('kafka: broker transport failure, retrying', ['reason' => $message->errstr()]);

        return null;
    }

    private function producer(): Producer
    {
        return $this->producer ??= $this->clients->producer(
            $this->options->brokers,
            $this->options->conf,
            $this->report(...),
        );
    }

    private function topic(): ProducerTopic
    {
        return $this->topic ??= $this->producer()->newTopic($this->options->topic);
    }

    private function subscribedConsumer(): KafkaConsumer
    {
        $consumer = $this->consumer();

        if (!$this->subscribed) {
            $consumer->subscribe([$this->options->topic]);
            $this->subscribed = true;
            $this->logger->debug('kafka: subscribed to topic "{topic}"', ['topic' => $this->options->topic]);
        }

        return $consumer;
    }

    private function consumer(): KafkaConsumer
    {
        if (null !== $this->consumer) {
            return $this->consumer;
        }

        $this->options->assertConsumable();

        return $this->consumer = $this->clients->consumer(
            $this->options->brokers,
            $this->options->consumerConf(),
            $this->rebalance(...),
        );
    }

    /** @param list<TopicPartition>|null $partitions */
    private function rebalance(KafkaConsumer $consumer, int $error, ?array $partitions = null): void
    {
        $partitions ??= [];

        match ($error) {
            \RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => $consumer->assign($partitions),
            \RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => $consumer->assign(null),
            default => throw new TransportException('kafka rebalance failed: '.rd_kafka_err2str($error), $error),
        };

        $this->logger->debug('kafka: partitions rebalanced', [
            'topic' => $this->options->topic,
            'partitions' => array_map(static fn (TopicPartition $p): int => $p->getPartition(), $partitions),
        ]);
    }
}

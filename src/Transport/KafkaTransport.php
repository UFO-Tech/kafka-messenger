<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Transport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\CloseableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Ufo\KafkaMessenger\Kafka\Connection;

class KafkaTransport implements TransportInterface, CloseableTransportInterface
{
    public function __construct(
        private KafkaSender $sender,
        private KafkaReceiver $receiver,
        private Connection $connection,
    ) {
    }

    public function get(int $fetchSize = 1): iterable
    {
        return $this->receiver->get($fetchSize);
    }

    public function ack(Envelope $envelope): void
    {
        $this->receiver->ack($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        $this->receiver->reject($envelope);
    }

    public function send(Envelope $envelope): Envelope
    {
        return $this->sender->send($envelope);
    }

    public function close(): void
    {
        $this->connection->close();
    }
}

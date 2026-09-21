<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Transport;

use Psr\Log\LoggerInterface;
use RdKafka\Message;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Ufo\KafkaMessenger\Kafka\Connection;
use Ufo\KafkaMessenger\Kafka\MessagePosition;
use Ufo\KafkaMessenger\Transport\Stamp\KafkaMessageStamp;

class KafkaReceiver implements ReceiverInterface
{
    public function __construct(
        private Connection $connection,
        private SerializerInterface $serializer,
        private LoggerInterface $logger,
    ) {
    }

    /** @return list<Envelope> */
    public function get(int $fetchSize = 1): iterable
    {
        $envelopes = [];

        foreach ($this->connection->consume(max(1, $fetchSize)) as $message) {
            $envelopes[] = $this->envelopeOf($message);
        }

        return $envelopes;
    }

    public function ack(Envelope $envelope): void
    {
        $this->connection->commit(self::messageOf($envelope));
    }

    public function reject(Envelope $envelope): void
    {
        $message = self::messageOf($envelope);

        $this->logger->warning('kafka: message rejected, offset left in place', MessagePosition::of($message));
    }

    private function envelopeOf(Message $message): Envelope
    {
        $encoded = [
            EncodedEnvelope::BODY => (string) $message->payload,
            /* @phpstan-ignore nullCoalesce.property (php-rdkafka leaves headers null for messages produced without them) */
            EncodedEnvelope::HEADERS => $message->headers ?? [],
        ];

        try {
            $envelope = $this->serializer->decode($encoded);
        } catch (MessageDecodingFailedException $failure) {
            $envelope = new Envelope($failure);
        }

        return $envelope->with(new KafkaMessageStamp($message));
    }

    /** @throws LogicException */
    private static function messageOf(Envelope $envelope): Message
    {
        $stamp = $envelope->last(KafkaMessageStamp::class);
        if (!$stamp instanceof KafkaMessageStamp) {
            throw new LogicException('envelope has no kafka message stamp: it was not received from this transport');
        }

        return $stamp->message;
    }
}

<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Transport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Ufo\KafkaMessenger\Kafka\Connection;
use Ufo\KafkaMessenger\Transport\Stamp\KafkaKeyStamp;

class KafkaSender implements SenderInterface
{
    public function __construct(
        private Connection $connection,
        private SerializerInterface $serializer,
    ) {
    }

    public function send(Envelope $envelope): Envelope
    {
        $encoded = $this->serializer->encode($envelope);

        $this->connection->publish(
            $encoded[EncodedEnvelope::BODY],
            self::headersOf($encoded),
            $envelope->last(KafkaKeyStamp::class)?->key,
        );

        return $envelope;
    }

    /**
     * @param array<string, mixed> $encoded
     *
     * @return array<string, string>
     */
    private static function headersOf(array $encoded): array
    {
        /** @var array<string, mixed> $headers */
        $headers = $encoded[EncodedEnvelope::HEADERS] ?? [];

        return array_map(strval(...), $headers);
    }
}

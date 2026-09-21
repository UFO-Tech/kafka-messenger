<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Support;

use RdKafka\ProducerTopic;

final class FakeProducerTopic extends ProducerTopic
{
    public function __construct(
        private readonly FakeProducer $producer,
    ) {
    }

    public function producev(
        int $partition,
        int $msgflags,
        ?string $payload = null,
        ?string $key = null,
        ?array $headers = null,
        ?int $timestamp_ms = null,
        $opaque = null,
    ): void {
        $this->producer->produced[] = [
            'payload' => (string) $payload,
            'key' => $key,
            'headers' => $headers ?? [],
        ];
    }
}

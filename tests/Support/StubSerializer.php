<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Support;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

final class StubSerializer implements SerializerInterface
{
    /** @var array<string, mixed> */
    public array $headers = [];

    public bool $failsToDecode = false;

    /** @param array<string, mixed> $encodedEnvelope */
    public function decode(array $encodedEnvelope): Envelope
    {
        if ($this->failsToDecode) {
            throw new MessageDecodingFailedException('stub serializer refuses to decode');
        }

        return (new PhpSerializer())->decode($encodedEnvelope);
    }

    /** @return array<string, mixed> */
    public function encode(Envelope $envelope): array
    {
        return (new PhpSerializer())->encode($envelope) + ['headers' => $this->headers];
    }
}

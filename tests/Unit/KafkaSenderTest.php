<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Ufo\KafkaMessenger\Tests\Support\Doubles;
use Ufo\KafkaMessenger\Tests\Support\StubSerializer;
use Ufo\KafkaMessenger\Transport\KafkaSender;
use Ufo\KafkaMessenger\Transport\Stamp\KafkaKeyStamp;

#[CoversClass(KafkaSender::class)]
final class KafkaSenderTest extends TestCase
{
    public function testMessageSurvivesTheSerializer(): void
    {
        $clients = Doubles::clients();
        Doubles::sender($clients)->send(new Envelope(new \stdClass()));

        $decoded = (new PhpSerializer())->decode(['body' => $clients->producer->produced[0]['payload']]);

        self::assertInstanceOf(\stdClass::class, $decoded->getMessage());
    }

    public function testPartitionKeyComesFromTheStamp(): void
    {
        $clients = Doubles::clients();
        Doubles::sender($clients)->send(new Envelope(new \stdClass(), [new KafkaKeyStamp('ufo-tech')]));

        self::assertSame('ufo-tech', $clients->producer->produced[0]['key']);
    }

    public function testWithoutAStampThereIsNoKey(): void
    {
        $clients = Doubles::clients();
        Doubles::sender($clients)->send(new Envelope(new \stdClass()));

        self::assertNull($clients->producer->produced[0]['key']);
    }

    public function testHeadersGoOutAsStrings(): void
    {
        $clients = Doubles::clients();
        $serializer = new StubSerializer();
        $serializer->headers = ['type' => 'stdClass', 'retries' => 3];

        Doubles::sender($clients, $serializer)->send(new Envelope(new \stdClass()));

        self::assertSame(['type' => 'stdClass', 'retries' => '3'], $clients->producer->produced[0]['headers']);
    }
}

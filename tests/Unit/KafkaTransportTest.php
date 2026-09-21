<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Ufo\KafkaMessenger\Tests\Support\Doubles;
use Ufo\KafkaMessenger\Tests\Support\FakeConsumer;
use Ufo\KafkaMessenger\Transport\KafkaTransport;

#[CoversClass(KafkaTransport::class)]
final class KafkaTransportTest extends TestCase
{
    public function testSendGoesThroughTheSender(): void
    {
        $clients = Doubles::clients();
        Doubles::transport($clients)->send(new Envelope(new \stdClass()));

        self::assertCount(1, $clients->producer->produced);
    }

    public function testGetAndAckGoThroughTheReceiver(): void
    {
        $clients = Doubles::clients(FakeConsumer::message(Doubles::encoded(), 7));
        $transport = Doubles::transport($clients);

        $transport->ack($transport->get()[0]);

        self::assertCount(1, $clients->consumer?->committed ?? []);
    }

    public function testRejectLeavesTheOffsetAlone(): void
    {
        $clients = Doubles::clients(FakeConsumer::message(Doubles::encoded()));
        $transport = Doubles::transport($clients);

        $transport->reject($transport->get()[0]);

        self::assertSame([], $clients->consumer?->committed);
    }

    public function testCloseReachesTheConsumer(): void
    {
        $clients = Doubles::clients(FakeConsumer::message(Doubles::encoded()));
        $transport = Doubles::transport($clients);
        $transport->get();

        $transport->close();

        self::assertTrue($clients->consumer?->closed);
    }
}

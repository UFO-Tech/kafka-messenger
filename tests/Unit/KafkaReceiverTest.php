<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Ufo\KafkaMessenger\Tests\Support\Doubles;
use Ufo\KafkaMessenger\Tests\Support\FakeConsumer;
use Ufo\KafkaMessenger\Tests\Support\StubSerializer;
use Ufo\KafkaMessenger\Transport\KafkaReceiver;
use Ufo\KafkaMessenger\Transport\Stamp\KafkaMessageStamp;

#[CoversClass(KafkaReceiver::class)]
final class KafkaReceiverTest extends TestCase
{
    public function testMessageBecomesAnEnvelopeCarryingTheRawMessage(): void
    {
        $clients = Doubles::clients(FakeConsumer::message(Doubles::encoded(), 42));

        $envelopes = Doubles::receiver($clients)->get();

        self::assertCount(1, $envelopes);
        self::assertInstanceOf(\stdClass::class, $envelopes[0]->getMessage());
        self::assertSame(42, $envelopes[0]->last(KafkaMessageStamp::class)?->message->offset);
    }

    public function testFetchSizeAsksForAWholeBatch(): void
    {
        $clients = Doubles::clients(
            FakeConsumer::message(Doubles::encoded()),
            FakeConsumer::message(Doubles::encoded()),
            FakeConsumer::message(Doubles::encoded()),
        );

        self::assertCount(2, Doubles::receiver($clients)->get(2));
    }

    public function testUnreadablePayloadTravelsAsAFailureInsteadOfJammingThePartition(): void
    {
        $clients = Doubles::clients(FakeConsumer::message('not a serialized envelope', 13));

        $envelopes = Doubles::receiver($clients)->get();

        self::assertInstanceOf(MessageDecodingFailedException::class, $envelopes[0]->getMessage());
        self::assertSame(13, $envelopes[0]->last(KafkaMessageStamp::class)?->message->offset);
    }

    public function testFailedEnvelopeStaysSerializableForTheFailureTransport(): void
    {
        $clients = Doubles::clients(FakeConsumer::message('not a serialized envelope'));

        $envelope = Doubles::receiver($clients)->get()[0];

        self::assertNotEmpty((new PhpSerializer())->encode($envelope)['body']);
    }

    public function testSerializerThatThrowsIsTreatedTheSameWay(): void
    {
        $clients = Doubles::clients(FakeConsumer::message(Doubles::encoded()));
        $serializer = new StubSerializer();
        $serializer->failsToDecode = true;

        $receiver = Doubles::receiver($clients, $serializer);
        $envelope = $receiver->get()[0];
        $receiver->ack($envelope);

        self::assertInstanceOf(MessageDecodingFailedException::class, $envelope->getMessage());
        self::assertCount(1, $clients->consumer?->committed ?? []);
    }

    public function testAckCommitsTheOffset(): void
    {
        $clients = Doubles::clients(FakeConsumer::message(Doubles::encoded(), 7));
        $receiver = Doubles::receiver($clients);

        $receiver->ack($receiver->get()[0]);

        self::assertCount(1, $clients->consumer?->committed ?? []);
    }

    public function testRejectCommitsNothing(): void
    {
        $clients = Doubles::clients(FakeConsumer::message(Doubles::encoded()));
        $receiver = Doubles::receiver($clients);

        $receiver->reject($receiver->get()[0]);

        self::assertSame([], $clients->consumer?->committed);
        self::assertSame([], $clients->consumer?->committedAsync);
    }

    public function testAckOfAForeignEnvelopeIsRefused(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('no kafka message stamp');

        Doubles::receiver(Doubles::clients())->ack(new Envelope(new \stdClass()));
    }
}

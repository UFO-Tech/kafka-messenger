<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Integration;

use Ufo\KafkaMessenger\Kafka\ClientFactory;
use Ufo\KafkaMessenger\Kafka\Connection;
use Ufo\KafkaMessenger\Kafka\Options;
use Ufo\KafkaMessenger\Transport\KafkaTransport;
use Ufo\KafkaMessenger\Transport\KafkaTransportFactory;
use Ufo\KafkaMessenger\Transport\Stamp\KafkaKeyStamp;
use Ufo\KafkaMessenger\Transport\Stamp\KafkaMessageStamp;
use PHPUnit\Framework\Attributes\CoversNothing;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

#[CoversNothing]
final class KafkaRoundTripTest extends TestCase
{
    private const int WAIT_MS = 15000;

    /** @var list<KafkaTransport> */
    private array $transports = [];

    protected function setUp(): void
    {
        if ('' === self::dsn()) {
            self::markTestSkipped('KAFKA_DSN is not set: no broker around, skipping the integration suite');
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->transports as $transport) {
            $transport->close();
        }

        $this->transports = [];
    }

    public function testMessageSurvivesTheRoundTrip(): void
    {
        $topic = self::topic();
        $sender = $this->transport($topic, 'producer');
        $receiver = $this->transport($topic, 'consumer');

        $sent = $sender->send(new Envelope(new SampleMessage('round trip'), [new KafkaKeyStamp('ufo-tech')]));
        self::assertInstanceOf(Envelope::class, $sent);

        $received = self::await($receiver);

        self::assertInstanceOf(SampleMessage::class, $received->getMessage());
        self::assertSame('round trip', $received->getMessage()->text);
    }

    public function testPartitionKeyReachesTheBroker(): void
    {
        $topic = self::topic();
        $this->transport($topic, 'producer')->send(new Envelope(new SampleMessage('keyed'), [new KafkaKeyStamp('ufo-tech')]));

        $message = self::await($this->transport($topic, 'consumer'))->last(KafkaMessageStamp::class)?->message;

        self::assertSame('ufo-tech', $message?->key);
    }

    public function testAckCommitsSoTheMessageIsNotReadTwice(): void
    {
        $topic = self::topic();
        $group = 'group-'.bin2hex(random_bytes(4));

        $this->transport($topic, 'producer')->send(new Envelope(new SampleMessage('once only')));

        $first = $this->transport($topic, 'consumer', $group);
        $envelope = self::await($first);
        $first->ack($envelope);

        self::assertNull(self::poll($this->transport($topic, 'consumer', $group), 3));
    }

    public function testUnreadablePayloadDoesNotJamThePartition(): void
    {
        $topic = self::topic();
        $group = 'group-'.bin2hex(random_bytes(4));

        self::rawPublisher($topic)->publish('not a serialized envelope', [], 'ufo-tech');
        $this->transport($topic, 'producer')->send(new Envelope(new SampleMessage('after the poison'), [new KafkaKeyStamp('ufo-tech')]));

        $consumer = $this->transport($topic, 'consumer', $group);
        $poison = self::await($consumer);

        self::assertInstanceOf(MessageDecodingFailedException::class, $poison->getMessage());

        $consumer->ack($poison);

        self::assertInstanceOf(SampleMessage::class, self::await($consumer)->getMessage());
    }

    private static function rawPublisher(string $topic): Connection
    {
        return new Connection(
            Options::fromDsn(self::dsn(), ['topic' => $topic]),
            new ClientFactory(),
            new NullLogger(),
        );
    }

    private static function await(KafkaTransport $transport): Envelope
    {
        return self::poll($transport, 5) ?? self::fail('the broker gave no message within '.self::WAIT_MS.' ms');
    }

    private static function poll(KafkaTransport $transport, int $attempts): ?Envelope
    {
        for ($i = 0; $i < $attempts; ++$i) {
            foreach ($transport->get() as $envelope) {
                return $envelope;
            }
        }

        return null;
    }

    private function transport(string $topic, string $role, ?string $group = null): KafkaTransport
    {
        $options = [
            'topic' => $topic,
            'receive_timeout' => self::WAIT_MS,
            'kafka_conf' => 'consumer' === $role
                ? [
                    'group.id' => $group ?? 'tests-'.bin2hex(random_bytes(4)),
                    'auto.offset.reset' => 'earliest',
                ]
                : [],
        ];

        $transport = (new KafkaTransportFactory(new ClientFactory()))
            ->createTransport(self::dsn(), $options, new PhpSerializer());

        \assert($transport instanceof KafkaTransport);

        return $this->transports[] = $transport;
    }

    private static function topic(): string
    {
        return 'tests-'.bin2hex(random_bytes(6));
    }

    private static function dsn(): string
    {
        return (string) (getenv('KAFKA_DSN') ?: '');
    }
}

final readonly class SampleMessage
{
    public function __construct(
        public string $text,
    ) {
    }
}

<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Ufo\KafkaMessenger\Kafka\ClientFactory;
use Ufo\KafkaMessenger\Transport\KafkaTransport;
use Ufo\KafkaMessenger\Transport\KafkaTransportFactory;

#[CoversClass(KafkaTransportFactory::class)]
final class KafkaTransportFactoryTest extends TestCase
{
    #[DataProvider('supported')]
    public function testFactoryClaimsOnlyKafkaDsns(string $dsn, bool $supported): void
    {
        self::assertSame($supported, self::factory()->supports($dsn, []));
    }

    /** @return iterable<string, array{string, bool}> */
    public static function supported(): iterable
    {
        yield 'kafka' => ['kafka://broker:9092', true];
        yield 'kafka+ssl' => ['kafka+ssl://broker:9093', true];
        yield 'amqp' => ['amqp://guest@broker:5672', false];
        yield 'doctrine' => ['doctrine://default', false];
        yield 'lookalike' => ['kafkaesque://broker:9092', false];
    }

    public function testTransportIsBuiltWithoutTouchingTheBroker(): void
    {
        $clients = $this->createMock(ClientFactory::class);
        $clients->expects(self::never())->method('producer');
        $clients->expects(self::never())->method('consumer');

        $transport = (new KafkaTransportFactory($clients))->createTransport(
            'kafka://broker:9092',
            ['topic' => 'events'],
            new PhpSerializer(),
        );

        self::assertInstanceOf(KafkaTransport::class, $transport);
    }

    public function testUnknownOptionIsIgnoredByDefault(): void
    {
        $transport = self::factory()->createTransport(
            'kafka://broker:9092/events?flushTimeout=1000',
            ['retries' => 3],
            new PhpSerializer(),
        );

        self::assertInstanceOf(KafkaTransport::class, $transport);
    }

    public function testStrictFactoryRefusesUnknownOption(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('retries');

        self::factory(strict: true)->createTransport('kafka://broker:9092/events', ['retries' => 3], new PhpSerializer());
    }

    public function testStrictFactoryAcceptsTransportNameAddedByFrameworkBundle(): void
    {
        $transport = self::factory(strict: true)->createTransport(
            'kafka://broker:9092/events',
            ['transport_name' => 'events'],
            new PhpSerializer(),
        );

        self::assertInstanceOf(KafkaTransport::class, $transport);
    }

    private static function factory(bool $strict = false): KafkaTransportFactory
    {
        return new KafkaTransportFactory(new ClientFactory(), strict: $strict);
    }
}

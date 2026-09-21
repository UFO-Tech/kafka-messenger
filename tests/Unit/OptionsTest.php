<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\LogicException;
use Ufo\KafkaMessenger\Kafka\Options;

#[CoversClass(Options::class)]
final class OptionsTest extends TestCase
{
    #[DataProvider('dsns')]
    public function testSchemeIsStrippedAndBrokersAreJoined(string $dsn, string $expected): void
    {
        self::assertSame($expected, self::options($dsn)->brokers);
    }

    /** @return iterable<string, array{string, string}> */
    public static function dsns(): iterable
    {
        yield 'single broker' => ['kafka://broker:9092', 'broker:9092'];
        yield 'ssl' => ['kafka+ssl://broker:9093', 'broker:9093'];
        yield 'scheme on each' => ['kafka://one:9092,kafka://two:9092', 'one:9092,two:9092'];
        yield 'scheme once' => ['kafka://one:9092,two:9092', 'one:9092,two:9092'];
        yield 'sasl+ssl' => ['kafka+sasl+ssl://one:9093,two:9093', 'one:9093,two:9093'];
        yield 'spaces' => ['kafka://one:9092, kafka://two:9092', 'one:9092,two:9092'];
    }

    public function testMixedSchemesAreRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('mixes schemes');

        self::options('kafka://one:9092,kafka+ssl://two:9093');
    }

    public function testDsnWithoutABrokerIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('carries no broker');

        self::options('kafka://');
    }

    public function testSslSchemeTurnsOnTheSecurityProtocol(): void
    {
        self::assertSame('ssl', self::options('kafka+ssl://broker:9093')->conf['security.protocol']);
    }

    public function testExplicitSecurityProtocolWins(): void
    {
        $options = self::options('kafka+ssl://broker:9093', ['security.protocol' => 'sasl_ssl']);

        self::assertSame('sasl_ssl', $options->conf['security.protocol']);
    }

    public function testEverySchemeNamesItsSecurityProtocol(): void
    {
        self::assertSame('plaintext', self::options()->conf['security.protocol']);
        self::assertSame('ssl', self::options('kafka+ssl://broker:9093')->conf['security.protocol']);
        self::assertSame('sasl_plaintext', self::options('kafka+sasl://broker:9092')->conf['security.protocol']);
        self::assertSame('sasl_ssl', self::options('kafka+sasl+ssl://broker:9093')->conf['security.protocol']);
    }

    public function testTopicIsRequired(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('needs a topic');

        Options::fromDsn('kafka://broker:9092', []);
    }

    public function testTopicIsAPlainString(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('needs a topic');

        Options::fromDsn('kafka://broker:9092', ['topic' => ['name' => 'events']]);
    }

    public function testUnknownOptionIsNamedOutLoud(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('flushTimeout');

        Options::fromDsn('kafka://broker:9092', ['topic' => 'events', 'flushTimeout' => 5000]);
    }

    public function testDefaultsAreConservative(): void
    {
        $options = self::options();

        self::assertSame(10000, $options->flushTimeout);
        self::assertSame(2, $options->flushRetries);
        self::assertSame(10000, $options->receiveTimeout);
        self::assertFalse($options->commitAsync);
    }

    #[DataProvider('timeouts')]
    public function testNegativeTimeoutIsRefused(string $option): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage($option);

        Options::fromDsn('kafka://broker:9092', ['topic' => 'events', $option => -1]);
    }

    /** @return iterable<string, array{string}> */
    public static function timeouts(): iterable
    {
        yield 'flush timeout' => ['flush_timeout'];
        yield 'flush retries' => ['flush_retries'];
        yield 'receive timeout' => ['receive_timeout'];
    }

    public function testKafkaConfIsPassedThroughAsStrings(): void
    {
        $options = self::options(conf: ['group.id' => 'g1', 'retries' => 5, 'enable.auto.commit' => false]);

        self::assertSame([
            'group.id' => 'g1',
            'retries' => '5',
            'enable.auto.commit' => 'false',
            'security.protocol' => 'plaintext',
        ], $options->conf);
    }

    public function testNestedKafkaConfIsRefused(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('kafka_conf, "group" got array');

        self::options(conf: ['group' => ['id' => 'g1']]);
    }

    public function testConsumerGetsAutoCommitOffByDefault(): void
    {
        self::assertSame('false', self::options(conf: ['group.id' => 'g1'])->consumerConf()['enable.auto.commit']);
    }

    public function testExplicitAutoCommitWins(): void
    {
        $options = self::options(conf: ['group.id' => 'g1', 'enable.auto.commit' => true]);

        self::assertSame('true', $options->consumerConf()['enable.auto.commit']);
    }

    public function testTheDefaultStaysOutOfTheSharedConf(): void
    {
        self::assertArrayNotHasKey('enable.auto.commit', self::options(conf: ['group.id' => 'g1'])->conf);
    }

    /** @param array<string, string> $conf */
    public function testConsumerConfCarriesOnlyStrings(): void
    {
        $conf = self::options(conf: ['group.id' => 'g1', 'retries' => 5, 'enable.idempotence' => true])->consumerConf();

        foreach ($conf as $name => $value) {
            self::assertIsString($value, \sprintf('librdkafka takes strings only, and "%s" is not one', $name));
        }
    }

    public function testConsumingWithoutGroupIdIsRefusedWithAReadableMessage(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('group.id');

        self::options()->assertConsumable();
    }

    public function testConsumingWithGroupIdIsAllowed(): void
    {
        self::options(conf: ['group.id' => 'g1'])->assertConsumable();

        $this->expectNotToPerformAssertions();
    }

    /** @param array<string, mixed> $conf */
    private static function options(string $dsn = 'kafka://broker:9092', array $conf = []): Options
    {
        return Options::fromDsn($dsn, ['topic' => 'events', 'kafka_conf' => $conf]);
    }
}

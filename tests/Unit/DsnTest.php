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
final class DsnTest extends TestCase
{
    public function testBareDsnGivesBrokersTopicAndPlaintext(): void
    {
        $options = Options::fromDsn('kafka://broker:9092/events', []);

        self::assertSame('broker:9092', $options->brokers);
        self::assertSame('events', $options->topic);
        self::assertSame('plaintext', $options->conf['security.protocol']);
    }

    #[DataProvider('brokerLists')]
    public function testBrokersSurviveEveryWayOfWritingThem(string $dsn, string $expected): void
    {
        self::assertSame($expected, Options::fromDsn($dsn, [])->brokers);
    }

    /** @return iterable<string, array{string, string}> */
    public static function brokerLists(): iterable
    {
        yield 'single' => ['kafka://one:9092/events', 'one:9092'];
        yield 'several' => ['kafka://one:9092,two:9092/events', 'one:9092,two:9092'];
        yield 'scheme on each' => ['kafka://one:9092,kafka://two:9092/events', 'one:9092,two:9092'];
        yield 'spaces' => ['kafka://one:9092, two:9092/events', 'one:9092,two:9092'];
        yield 'ssl on several' => ['kafka+ssl://one:9093,two:9093/events', 'one:9093,two:9093'];
    }

    #[DataProvider('protocols')]
    public function testSchemeDecidesTheSecurityProtocol(string $dsn, string $protocol): void
    {
        self::assertSame($protocol, Options::fromDsn($dsn, [])->conf['security.protocol']);
    }

    /** @return iterable<string, array{string, string}> */
    public static function protocols(): iterable
    {
        yield 'plaintext' => ['kafka://b:9092/events', 'plaintext'];
        yield 'ssl' => ['kafka+ssl://b:9093/events', 'ssl'];
        yield 'sasl' => ['kafka+sasl://u:p@b:9092/events', 'sasl_plaintext'];
        yield 'sasl+ssl' => ['kafka+sasl+ssl://u:p@b:9098/events', 'sasl_ssl'];
    }

    public function testUserInfoBecomesSaslCredentials(): void
    {
        $conf = Options::fromDsn('kafka+sasl+ssl://svc-user:s3cret@broker:9098/events', [])->conf;

        self::assertSame('svc-user', $conf['sasl.username']);
        self::assertSame('s3cret', $conf['sasl.password']);
    }

    public function testEncodedPasswordIsDecoded(): void
    {
        $conf = Options::fromDsn('kafka+sasl+ssl://user:p%40ss%3Aword%2F1@broker:9098/events', [])->conf;

        self::assertSame('p@ss:word/1', $conf['sasl.password']);
    }

    public function testUserWithoutPasswordIsAccepted(): void
    {
        $conf = Options::fromDsn('kafka+sasl://lonely@broker:9092/events', [])->conf;

        self::assertSame('lonely', $conf['sasl.username']);
        self::assertArrayNotHasKey('sasl.password', $conf);
    }

    public function testCredentialsWithoutSaslSchemeAreRefused(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('kafka+sasl');

        Options::fromDsn('kafka://user:pass@broker:9092/events', []);
    }

    public function testDottedQueryKeysGoToKafkaConf(): void
    {
        $conf = Options::fromDsn('kafka://b:9092/events?group.id=svc&auto.offset.reset=earliest', [])->conf;

        self::assertSame('svc', $conf['group.id']);
        self::assertSame('earliest', $conf['auto.offset.reset']);
    }

    public function testPlainQueryKeysAreTransportOptions(): void
    {
        $options = Options::fromDsn('kafka://b:9092/events?flush_timeout=5000&flush_retries=1&receive_timeout=2000&commit_async=true', []);

        self::assertSame(5000, $options->flushTimeout);
        self::assertSame(1, $options->flushRetries);
        self::assertSame(2000, $options->receiveTimeout);
        self::assertTrue($options->commitAsync);
    }

    #[DataProvider('booleans')]
    public function testBooleanOptionsAcceptQueryStringSpelling(string $written, bool $expected): void
    {
        $options = Options::fromDsn(\sprintf('kafka://b:9092/events?commit_async=%s', $written), []);

        self::assertSame($expected, $options->commitAsync);
    }

    /** @return iterable<string, array{string, bool}> */
    public static function booleans(): iterable
    {
        yield 'true' => ['true', true];
        yield 'false' => ['false', false];
        yield 'one' => ['1', true];
        yield 'zero' => ['0', false];
    }

    public function testTopicWithDotsSurvivesThePath(): void
    {
        self::assertSame('orders.created.v1', Options::fromDsn('kafka://b:9092/orders.created.v1', [])->topic);
    }

    public function testDsnBeatsTheOptionsArray(): void
    {
        $options = Options::fromDsn('kafka://b:9092/from-dsn?flush_timeout=1000', [
            'topic' => 'from-options',
            'flush_timeout' => 9000,
        ]);

        self::assertSame('from-dsn', $options->topic);
        self::assertSame(1000, $options->flushTimeout);
    }

    public function testOptionsArrayStillWorksOnItsOwn(): void
    {
        $options = Options::fromDsn('kafka://b:9092', [
            'topic' => 'events',
            'flush_timeout' => 3000,
            'kafka_conf' => ['group.id' => 'svc'],
        ]);

        self::assertSame('events', $options->topic);
        self::assertSame(3000, $options->flushTimeout);
        self::assertSame('svc', $options->conf['group.id']);
    }

    public function testQueryAndOptionsMergeInsideKafkaConf(): void
    {
        $conf = Options::fromDsn('kafka://b:9092/events?group.id=from-dsn', [
            'kafka_conf' => ['group.id' => 'from-options', 'auto.offset.reset' => 'earliest'],
        ])->conf;

        self::assertSame('from-dsn', $conf['group.id']);
        self::assertSame('earliest', $conf['auto.offset.reset']);
    }

    public function testUnknownPlainQueryKeyIsRefused(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('flushTimeout');

        Options::fromDsn('kafka://b:9092/events?flushTimeout=1000', []);
    }

    public function testMissingTopicIsStillRefused(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('needs a topic');

        Options::fromDsn('kafka://b:9092', []);
    }

    public function testMixedSchemesAreStillRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('mixes schemes');

        Options::fromDsn('kafka://one:9092,kafka+ssl://two:9093/events', []);
    }

    public function testEmptyPathIsNotATopic(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('needs a topic');

        Options::fromDsn('kafka://b:9092/?group.id=svc', []);
    }
}

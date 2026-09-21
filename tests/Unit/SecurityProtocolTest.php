<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Ufo\KafkaMessenger\Kafka\SecurityProtocol;

#[CoversClass(SecurityProtocol::class)]
final class SecurityProtocolTest extends TestCase
{
    #[DataProvider('schemes')]
    public function testSchemeIsDerivedFromTheProtocol(SecurityProtocol $protocol, string $scheme): void
    {
        self::assertSame($scheme, $protocol->scheme());
    }

    /** @return iterable<string, array{SecurityProtocol, string}> */
    public static function schemes(): iterable
    {
        yield 'plaintext' => [SecurityProtocol::Plaintext, 'kafka://'];
        yield 'ssl' => [SecurityProtocol::Ssl, 'kafka+ssl://'];
        yield 'sasl' => [SecurityProtocol::SaslPlaintext, 'kafka+sasl://'];
        yield 'sasl+ssl' => [SecurityProtocol::SaslSsl, 'kafka+sasl+ssl://'];
    }

    #[DataProvider('claims')]
    public function testOnlyKafkaDsnsAreClaimed(string $dsn, bool $supported): void
    {
        self::assertSame($supported, SecurityProtocol::supports($dsn));
    }

    /** @return iterable<string, array{string, bool}> */
    public static function claims(): iterable
    {
        yield 'kafka' => ['kafka://broker:9092', true];
        yield 'kafka+ssl' => ['kafka+ssl://broker:9093', true];
        yield 'kafka+sasl' => ['kafka+sasl://broker:9092', true];
        yield 'kafka+sasl+ssl' => ['kafka+sasl+ssl://broker:9093', true];
        yield 'amqp' => ['amqp://guest@broker:5672', false];
        yield 'lookalike' => ['kafkaesque://broker:9092', false];
    }

    #[DataProvider('brokers')]
    public function testProtocolIsRecognisedAndStripped(string $broker, ?SecurityProtocol $expected, string $stripped): void
    {
        $protocol = SecurityProtocol::of($broker);

        self::assertSame($expected, $protocol);
        self::assertSame($stripped, $protocol?->strip($broker) ?? $broker);
    }

    /** @return iterable<string, array{string, ?SecurityProtocol, string}> */
    public static function brokers(): iterable
    {
        yield 'plaintext' => ['kafka://one:9092', SecurityProtocol::Plaintext, 'one:9092'];
        yield 'ssl' => ['kafka+ssl://one:9093', SecurityProtocol::Ssl, 'one:9093'];
        yield 'sasl' => ['kafka+sasl://one:9092', SecurityProtocol::SaslPlaintext, 'one:9092'];
        yield 'sasl+ssl' => ['kafka+sasl+ssl://one:9093', SecurityProtocol::SaslSsl, 'one:9093'];
        yield 'no scheme' => ['one:9092', null, 'one:9092'];
    }

    public function testLongerSchemeWinsOverItsPrefix(): void
    {
        self::assertSame(SecurityProtocol::SaslSsl, SecurityProtocol::of('kafka+sasl+ssl://broker:9093'));
        self::assertSame(SecurityProtocol::SaslPlaintext, SecurityProtocol::of('kafka+sasl://broker:9092'));
    }
}

<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Kafka;

use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\LogicException;

final readonly class Dsn
{
    private const BROKER_SEPARATOR = ',';
    private const QUERY_SEPARATOR = '?';
    private const PAIR_SEPARATOR = '&';
    private const VALUE_SEPARATOR = '=';
    private const TOPIC_SEPARATOR = '/';
    private const USER_SEPARATOR = '@';
    private const PASSWORD_SEPARATOR = ':';
    private const CONF_MARKER = '.';

    /**
     * @param array<string, string> $options
     * @param array<string, string> $conf
     */
    private function __construct(
        public string $brokers,
        public ?SecurityProtocol $protocol,
        public ?string $topic,
        public array $options,
        public array $conf,
    ) {
    }

    /**
     * @throws InvalidArgumentException
     * @throws LogicException
     */
    public static function parse(string $dsn): self
    {
        [$head, $query] = array_pad(explode(self::QUERY_SEPARATOR, $dsn, 2), 2, '');

        $brokers = [];
        $schemes = [];
        $protocol = null;
        $topic = null;
        $credentials = [];

        foreach (explode(self::BROKER_SEPARATOR, $head) as $part) {
            $broker = trim($part);

            if (null !== ($scheme = SecurityProtocol::of($broker))) {
                $schemes[$scheme->value] = true;
                $protocol ??= $scheme;
                $broker = $scheme->strip($broker);
            }

            if (str_contains($broker, self::TOPIC_SEPARATOR)) {
                [$broker, $topic] = explode(self::TOPIC_SEPARATOR, $broker, 2);
            }

            if (false !== ($at = strrpos($broker, self::USER_SEPARATOR))) {
                $credentials = self::credentialsOf(substr($broker, 0, $at), $protocol, $dsn);
                $broker = substr($broker, $at + 1);
            }

            if ('' !== $broker) {
                $brokers[] = $broker;
            }
        }

        if (\count($schemes) > 1) {
            throw new InvalidArgumentException(\sprintf('kafka dsn mixes schemes: %s covers the whole client, not one broker', ConfProperty::SecurityProtocol->value));
        }

        if ([] === $brokers) {
            throw new InvalidArgumentException(\sprintf('kafka dsn "%s" carries no broker: expected %shost:9092', $dsn, SecurityProtocol::Plaintext->scheme()));
        }

        [$options, $conf] = self::queryOf($query);

        return new self(
            brokers: implode(self::BROKER_SEPARATOR, $brokers),
            protocol: $protocol,
            topic: '' === $topic ? null : $topic,
            options: $options,
            conf: $credentials + $conf,
        );
    }

    /**
     * @return array{array<string, string>, array<string, string>}
     */
    private static function queryOf(string $query): array
    {
        $options = [];
        $conf = [];

        foreach (explode(self::PAIR_SEPARATOR, $query) as $pair) {
            if ('' === $pair) {
                continue;
            }

            [$name, $value] = array_pad(explode(self::VALUE_SEPARATOR, $pair, 2), 2, '');
            $name = rawurldecode($name);
            $value = rawurldecode($value);

            if (str_contains($name, self::CONF_MARKER)) {
                $conf[$name] = $value;
            } else {
                $options[$name] = $value;
            }
        }

        return [$options, $conf];
    }

    /**
     * @return array<string, string>
     *
     * @throws LogicException
     */
    private static function credentialsOf(string $userInfo, ?SecurityProtocol $protocol, string $dsn): array
    {
        if (null === $protocol || !$protocol->authenticates()) {
            throw new LogicException(\sprintf(
                'kafka dsn carries credentials but its scheme does not authenticate: use %s or %s',
                SecurityProtocol::SaslPlaintext->scheme(),
                SecurityProtocol::SaslSsl->scheme(),
            ));
        }

        [$user, $password] = array_pad(explode(self::PASSWORD_SEPARATOR, $userInfo, 2), 2, '');

        $credentials = [ConfProperty::SaslUsername->value => rawurldecode($user)];
        if ('' !== $password) {
            $credentials[ConfProperty::SaslPassword->value] = rawurldecode($password);
        }

        return $credentials;
    }
}

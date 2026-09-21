<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Kafka;

use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\LogicException;

readonly class Options
{
    private const DEFAULT_FLUSH_TIMEOUT_MS = 10000;
    private const DEFAULT_FLUSH_RETRIES = 2;
    private const DEFAULT_RECEIVE_TIMEOUT_MS = 10000;

    /** @param array<string, string> $conf */
    private function __construct(
        public string $brokers,
        public string $topic,
        public array $conf,
        public int $flushTimeout = self::DEFAULT_FLUSH_TIMEOUT_MS,
        public int $flushRetries = self::DEFAULT_FLUSH_RETRIES,
        public int $receiveTimeout = self::DEFAULT_RECEIVE_TIMEOUT_MS,
        public bool $commitAsync = true,
    ) {
    }

    /**
     * @param array<string, mixed> $options
     *
     * @throws InvalidArgumentException
     * @throws LogicException
     */
    public static function fromDsn(string $dsn, array $options): self
    {
        $parsed = Dsn::parse($dsn);
        $options = $parsed->options + $options;

        if (null !== $parsed->topic) {
            $options[TransportOption::Topic->value] = $parsed->topic;
        }

        self::assertKnown($options);

        $topic = $options[TransportOption::Topic->value] ?? null;
        if (!\is_string($topic) || '' === $topic) {
            throw new LogicException('kafka transport needs a topic: name it in the dsn path or in options.topic');
        }

        return new self(
            brokers: $parsed->brokers,
            topic: $topic,
            conf: self::confOf($options, $parsed),
            flushTimeout: self::nonNegative($options, TransportOption::FlushTimeout, self::DEFAULT_FLUSH_TIMEOUT_MS),
            flushRetries: self::nonNegative($options, TransportOption::FlushRetries, self::DEFAULT_FLUSH_RETRIES),
            receiveTimeout: self::nonNegative($options, TransportOption::ReceiveTimeout, self::DEFAULT_RECEIVE_TIMEOUT_MS),
            commitAsync: self::flag($options, TransportOption::CommitAsync),
        );
    }

    /**
     * @return array<string, string>
     */
    public function consumerConf(): array
    {
        return $this->conf + [ConfProperty::AutoCommit->value => 'false'];
    }

    /** @throws LogicException */
    public function assertConsumable(): void
    {
        if (!isset($this->conf[ConfProperty::GroupId->value])) {
            throw new LogicException(\sprintf('kafka transport of topic "%s" cannot consume: set options.%s."%s"', $this->topic, TransportOption::KafkaConf->value, ConfProperty::GroupId->value));
        }
    }

    /**
     * @param array<string, mixed> $options
     *
     * @throws LogicException
     */
    private static function nonNegative(array $options, TransportOption $option, int $default): int
    {
        $value = (int) ($options[$option->value] ?? $default);
        if ($value < 0) {
            throw new LogicException(\sprintf('kafka transport takes a non-negative options.%s, got %d: librdkafka reads a negative timeout as "wait forever"', $option->value, $value));
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $options
     */
    private static function flag(array $options, TransportOption $option): bool
    {
        return filter_var($options[$option->value] ?? false, \FILTER_VALIDATE_BOOL);
    }

    /**
     * @param array<string, mixed> $options
     *
     * @throws LogicException
     */
    private static function assertKnown(array $options): void
    {
        $unknown = array_diff(array_keys($options), TransportOption::names());
        if ([] === $unknown) {
            return;
        }

        throw new LogicException(\sprintf(
            'kafka transport got unknown option(s) "%s"; it takes "%s"',
            implode('", "', $unknown),
            implode('", "', TransportOption::names()),
        ));
    }

    /**
     * @param array<string, mixed> $options
     *
     * @return array<string, string>
     *
     * @throws LogicException
     */
    private static function confOf(array $options, Dsn $dsn): array
    {
        $conf = [];

        foreach ((array) ($options[TransportOption::KafkaConf->value] ?? []) as $name => $value) {
            if (!\is_scalar($value)) {
                throw new LogicException(\sprintf('kafka transport takes scalars in options.kafka_conf, "%s" got %s', $name, get_debug_type($value)));
            }

            $conf[(string) $name] = \is_bool($value) ? ($value ? 'true' : 'false') : (string) $value;
        }

        $conf = $dsn->conf + $conf;

        if (null !== $dsn->protocol && !isset($conf[ConfProperty::SecurityProtocol->value])) {
            $conf[ConfProperty::SecurityProtocol->value] = $dsn->protocol->value;
        }

        return $conf;
    }

    /**
     * @param array<string, mixed> $options
     *
     * @throws LogicException
     */
}

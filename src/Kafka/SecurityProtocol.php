<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Kafka;

enum SecurityProtocol: string
{
    private const SCHEME = 'kafka';
    private const PART_SEPARATOR = '+';
    private const VALUE_SEPARATOR = '_';
    private const SCHEME_SUFFIX = '://';

    case Plaintext = 'plaintext';
    case Ssl = 'ssl';
    case SaslPlaintext = 'sasl_plaintext';
    case SaslSsl = 'sasl_ssl';

    public static function supports(string $dsn): bool
    {
        return null !== self::of($dsn);
    }

    public static function of(string $broker): ?self
    {
        foreach (self::cases() as $protocol) {
            if ($protocol->starts($broker)) {
                return $protocol;
            }
        }

        return null;
    }

    /** kafka://, kafka+ssl://, kafka+sasl://, kafka+sasl+ssl:// */
    public function scheme(): string
    {
        $parts = array_filter(
            explode(self::VALUE_SEPARATOR, $this->value),
            static fn (string $part): bool => self::Plaintext->value !== $part,
        );

        $suffix = array_map(static fn (string $part): string => self::PART_SEPARATOR.$part, $parts);

        return self::SCHEME.implode('', $suffix).self::SCHEME_SUFFIX;
    }

    public function authenticates(): bool
    {
        return match ($this) {
            self::SaslPlaintext, self::SaslSsl => true,
            self::Plaintext, self::Ssl => false,
        };
    }

    public function strip(string $broker): string
    {
        return substr($broker, \strlen($this->scheme()));
    }

    private function starts(string $dsn): bool
    {
        return str_starts_with($dsn, $this->scheme());
    }
}

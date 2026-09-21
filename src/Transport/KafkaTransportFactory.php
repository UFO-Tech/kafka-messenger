<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Transport;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Ufo\KafkaMessenger\Kafka\ClientFactory;
use Ufo\KafkaMessenger\Kafka\Connection;
use Ufo\KafkaMessenger\Kafka\Options;
use Ufo\KafkaMessenger\Kafka\SecurityProtocol;

/** @implements TransportFactoryInterface<KafkaTransport> */
final readonly class KafkaTransportFactory implements TransportFactoryInterface
{
    private LoggerInterface $logger;

    public function __construct(
        private ClientFactory $clients,
        ?LoggerInterface $logger = null,
    ) {
        $this->logger = $logger ?? new NullLogger();
    }

    /** @param array<string, mixed> $options */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $parsed = Options::fromDsn($dsn, $options);
        $connection = new Connection($parsed, $this->clients, $this->logger);

        return new KafkaTransport(
            new KafkaSender($connection, $serializer),
            new KafkaReceiver($connection, $serializer, $this->logger),
            $connection,
        );
    }

    /** @param array<string, mixed> $options */
    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return SecurityProtocol::supports($dsn);
    }
}

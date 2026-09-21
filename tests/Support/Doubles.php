<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Support;

use Psr\Log\NullLogger;
use RdKafka\Message;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Ufo\KafkaMessenger\Kafka\Connection;
use Ufo\KafkaMessenger\Kafka\Options;
use Ufo\KafkaMessenger\Transport\KafkaReceiver;
use Ufo\KafkaMessenger\Transport\KafkaSender;
use Ufo\KafkaMessenger\Transport\KafkaTransport;

final class Doubles
{
    public static function clients(Message ...$queue): FakeClientFactory
    {
        $clients = new FakeClientFactory();
        $clients->consumer = new FakeConsumer();
        $clients->consumer->queue = array_values($queue);

        return $clients;
    }

    /** @param array<string, mixed> $extra */
    public static function connection(FakeClientFactory $clients, array $extra = []): Connection
    {
        return new Connection(
            Options::fromDsn('kafka://broker:9092', ['topic' => 'events'] + $extra),
            $clients,
            new NullLogger(),
        );
    }

    /** @param array<string, mixed> $extra */
    public static function consuming(FakeClientFactory $clients, array $extra = []): Connection
    {
        return self::connection($clients, ['kafka_conf' => ['group.id' => 'tests']] + $extra);
    }

    public static function sender(FakeClientFactory $clients, ?SerializerInterface $serializer = null): KafkaSender
    {
        return new KafkaSender(self::consuming($clients), $serializer ?? new PhpSerializer());
    }

    public static function receiver(FakeClientFactory $clients, ?SerializerInterface $serializer = null): KafkaReceiver
    {
        return new KafkaReceiver(self::consuming($clients), $serializer ?? new PhpSerializer(), new NullLogger());
    }

    public static function transport(FakeClientFactory $clients): KafkaTransport
    {
        $connection = self::consuming($clients);
        $serializer = new PhpSerializer();

        return new KafkaTransport(
            new KafkaSender($connection, $serializer),
            new KafkaReceiver($connection, $serializer, new NullLogger()),
            $connection,
        );
    }

    public static function encoded(): string
    {
        return (new PhpSerializer())->encode(new Envelope(new \stdClass()))['body'];
    }
}

<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Ufo\KafkaMessenger\Kafka\ClientFactory;
use Ufo\KafkaMessenger\Transport\KafkaTransportFactory;
use Ufo\KafkaMessenger\UfoKafkaMessengerBundle;

#[CoversClass(UfoKafkaMessengerBundle::class)]
final class UfoKafkaMessengerBundleTest extends TestCase
{
    public function testBundleRegistersTheTransportFactory(): void
    {
        $container = self::loaded();

        self::assertTrue($container->hasDefinition(ClientFactory::class));
        self::assertArrayHasKey(
            'messenger.transport_factory',
            $container->getDefinition(KafkaTransportFactory::class)->getTags(),
        );
    }

    public function testContainerCompilesWithoutALogger(): void
    {
        $container = self::loaded();
        $container->getDefinition(KafkaTransportFactory::class)->setPublic(true);

        $container->compile();

        self::assertInstanceOf(KafkaTransportFactory::class, $container->get(KafkaTransportFactory::class));
    }

    private static function loaded(): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.debug', false);
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());

        (new UfoKafkaMessengerBundle())->getContainerExtension()?->load([], $container);

        return $container;
    }
}

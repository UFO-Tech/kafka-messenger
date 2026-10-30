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

    public function testOptionsAreNotStrictByDefault(): void
    {
        self::assertFalse(self::loaded()->getDefinition(KafkaTransportFactory::class)->getArgument('$strict'));
    }

    public function testStrictOptionsAreSwitchedOnByConfig(): void
    {
        $container = self::loaded(['strict_options' => true]);

        self::assertTrue($container->getDefinition(KafkaTransportFactory::class)->getArgument('$strict'));
    }

    /** @param array<string, mixed> $config */
    private static function loaded(array $config = []): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.debug', false);
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());

        (new UfoKafkaMessengerBundle())->getContainerExtension()?->load([$config], $container);

        return $container;
    }
}

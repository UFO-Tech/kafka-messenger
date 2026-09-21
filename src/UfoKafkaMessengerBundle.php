<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;
use Ufo\KafkaMessenger\Kafka\ClientFactory;
use Ufo\KafkaMessenger\Transport\KafkaTransportFactory;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

final class UfoKafkaMessengerBundle extends AbstractBundle
{
    private const TRANSPORT_FACTORY_TAG = 'messenger.transport_factory';
    private const LOGGER_SERVICE = 'logger';

    /** @param array<array-key, mixed> $config */
    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $container->services()
            ->set(ClientFactory::class)
            ->private()

            ->set(KafkaTransportFactory::class)
            ->private()
            ->args([service(ClientFactory::class), service(self::LOGGER_SERVICE)->nullOnInvalid()])
            ->tag(self::TRANSPORT_FACTORY_TAG)
        ;
    }
}

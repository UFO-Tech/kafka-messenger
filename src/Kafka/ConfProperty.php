<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Kafka;

enum ConfProperty: string
{
    case BrokerList = 'metadata.broker.list';
    case GroupId = 'group.id';
    case AutoCommit = 'enable.auto.commit';
    case SecurityProtocol = 'security.protocol';
    case SaslUsername = 'sasl.username';
    case SaslPassword = 'sasl.password';
}

# KafkaMessenger
![Ukraine](https://img.shields.io/badge/Glory-Ukraine-yellow?labelColor=blue)

Kafka transport for Symfony Messenger, built directly on librdkafka

### About this package

This package lets a Symfony application send and consume Kafka messages through the Messenger component.

>A message that Kafka never acknowledged should never look delivered.

![License](https://img.shields.io/badge/license-MIT-green?labelColor=7b8185)
![Size](https://img.shields.io/github/repo-size/ufo-tech/kafka-messenger?label=Size%20of%20the%20repository)
![package_version](https://img.shields.io/github/v/tag/ufo-tech/kafka-messenger?color=blue&label=Latest%20Version&logo=Packagist&logoColor=white&labelColor=7b8185)
![fork](https://img.shields.io/github/forks/ufo-tech/kafka-messenger?color=green&logo=github&style=flat)

### Environment Requirements
![php_version](https://img.shields.io/packagist/dependency-v/ufo-tech/kafka-messenger/php?logo=PHP&logoColor=white)
![symfony_version](https://img.shields.io/packagist/dependency-v/ufo-tech/kafka-messenger/symfony/messenger?label=Symfony%20Messenger&logo=Symfony&logoColor=white)

PHP 8.2 or newer with `ext-rdkafka`, and Symfony Messenger 7.3 or newer. One tag covers both lines — the suite is run against 7.3 and against 8.1, with the same code. Beyond Messenger itself the package pulls in nothing: `psr/log` and four Symfony components, no HTTP client, no serializer, no schema registry.

## Installation

```console
composer require ufo-tech/kafka-messenger
```

Symfony Flex registers the bundle. Without Flex, add it to `config/bundles.php`:

```php
Ufo\KafkaMessenger\UfoKafkaMessengerBundle::class => ['all' => true],
```

## Configuration

```yaml
framework:
    messenger:
        transports:
            events:
                dsn: '%env(KAFKA_DSN)%'
                options:
                    topic: 'orders.created'
                    flush_timeout: 10000      # how long to wait for a delivery report, ms
                    flush_retries: 2          # how many times to repeat the flush
                    receive_timeout: 10000    # how long to wait for a message, ms
                    commit_async: false       # commit offsets without waiting for the broker
                    kafka_conf:               # everything else goes to librdkafka as it is
                        group.id: 'orders-service'
                        auto.offset.reset: 'earliest'
```

Any other key stops the transport at startup and the error names the ones it accepts. Values in `kafka_conf` are turned into strings for you, so a YAML `false` reaches librdkafka as `"false"` rather than as an empty value.

A consumer needs `group.id`: without it there is nowhere to keep the offset, and the transport says so instead of letting librdkafka abort. `enable.auto.commit` is set to `false` unless you choose otherwise — with auto-commit on, a background thread moves the offset and neither `ack` nor `reject` decides anything any more.

## DSN

The whole configuration fits in the connection string:

```
kafka+sasl+ssl://user:pass@b-1:9098,b-2:9098/orders.created?group.id=orders-service&flush_timeout=5000
```

| Part | Becomes |
|---|---|
| scheme | `security.protocol` |
| `user:pass` | `sasl.username` and `sasl.password`, accepted only on a `kafka+sasl…` scheme |
| hosts, comma separated | the broker list |
| path | the topic |
| query key **with a dot** | a librdkafka property, same as `kafka_conf` |
| query key **without a dot** | a transport option, checked against the same list as above |

What the DSN says wins over the options array, the way it does in Symfony's own transports.

| Scheme | `security.protocol` |
|---|---|
| `kafka://` | `plaintext` |
| `kafka+ssl://` | `ssl` |
| `kafka+sasl://` | `sasl_plaintext` |
| `kafka+sasl+ssl://` | `sasl_ssl` |

A value you set in `kafka_conf` always beats the scheme. Mixing schemes in one DSN is an error: `security.protocol` covers the whole client, not one broker.

## Partition key

Messages sharing a key land in the same partition and keep their order:

```php
$bus->dispatch(new OrderPlaced($orderId), [new KafkaKeyStamp($orderId)]);
```

## Batches and unreadable messages

The receiver hands back a batch whenever the worker asks for one: the first message waits for `receive_timeout`, the rest are collected without waiting, as many as are already buffered. Symfony's worker learned to ask (`messenger:consume --fetch-size=10`) in 8.1, so on 7.3 and 8.0 every call brings a single message.

A message the serializer cannot decode comes back as an envelope carrying `MessageDecodingFailedException`, the shape `ReceiverInterface` asks a transport for. The worker routes it through the usual retry and failure path and acks it, so the offset moves on and one bad message cannot stop a partition.

What the failure transport keeps depends on the Symfony line. From 8.1 its serializers return that exception with the original encoded envelope inside it, so the original body travels with it into the failure transport and `messenger:failed:retry` decodes it again once the reason is fixed. On 7.3 and 8.0 the serializer throws instead, the exception has nowhere to carry the payload, and the failure transport keeps the exception alone.

## How it works

`Kafka\Connection` is the only place that talks to librdkafka: clients, delivery reports, offsets, rebalances, leaving the group. `Transport\KafkaSender` and `Transport\KafkaReceiver` speak only Messenger — serialization, stamps, ack and reject — and share one connection. `Kafka\Dsn` and `Kafka\Options` parse the configuration and refuse what they do not recognise.

The transport implements `CloseableTransportInterface`: when a worker stops, the consumer leaves its group at once and the partitions move to its neighbours immediately instead of after `session.timeout.ms`.

## [More from UFO-Tech](https://packagist.org/packages/ufo-tech/)
This and seventeen other packages published under the [ufo-tech](https://packagist.org/packages/ufo-tech/) vendor on Packagist.

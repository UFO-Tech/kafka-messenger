# KafkaMessenger
![Ukraine](https://img.shields.io/badge/Glory-Ukraine-yellow?labelColor=blue)

Транспорт Kafka для Symfony Messenger напряму через librdkafka

### Про цей пакет

Пакет дозволяє застосунку на Symfony відправляти й читати повідомлення Kafka через компонент Messenger.

>Повідомлення, якого Kafka не підтвердила, не має виглядати доставленим.

![License](https://img.shields.io/badge/license-MIT-green?labelColor=7b8185)
![Size](https://img.shields.io/github/repo-size/ufo-tech/kafka-messenger?label=Size%20of%20the%20repository)
![package_version](https://img.shields.io/github/v/tag/ufo-tech/kafka-messenger?color=blue&label=Latest%20Version&logo=Packagist&logoColor=white&labelColor=7b8185)
![fork](https://img.shields.io/github/forks/ufo-tech/kafka-messenger?color=green&logo=github&style=flat)

### Вимоги до оточення
![php_version](https://img.shields.io/packagist/dependency-v/ufo-tech/kafka-messenger/php?logo=PHP&logoColor=white)
![symfony_version](https://img.shields.io/packagist/dependency-v/ufo-tech/kafka-messenger/symfony/messenger?label=Symfony%20Messenger&logo=Symfony&logoColor=white)

PHP 8.2 або новіший із `ext-rdkafka` та Symfony Messenger 7.3 або новіший. Один тег закриває обидві лінії — набір тестів ганяється і проти 7.3, і проти 8.1, тим самим кодом. Понад сам месенджер пакет не тягне нічого: `psr/log` і чотири компоненти Symfony, без HTTP-клієнта, серіалізаторів і реєстру схем.

## Встановлення

```console
composer require ufo-tech/kafka-messenger
```

Бандл підхоплює Symfony Flex. Без Flex додайте його в `config/bundles.php`:

```php
Ufo\KafkaMessenger\UfoKafkaMessengerBundle::class => ['all' => true],
```

## Налаштування

```yaml
framework:
    messenger:
        transports:
            events:
                dsn: '%env(KAFKA_DSN)%'
                options:
                    topic: 'orders.created'
                    flush_timeout: 10000      # скільки чекати підтвердження доставки, мс
                    flush_retries: 2          # скільки разів повторити флаш
                    receive_timeout: 10000    # скільки чекати повідомлення, мс
                    commit_async: false       # коміт зсуву без очікування брокера
                    kafka_conf:               # усе інше йде в librdkafka як є
                        group.id: 'orders-service'
                        auto.offset.reset: 'earliest'
```

Будь-який інший ключ зупиняє транспорт на старті, і помилка називає ті, які він приймає. Значення в `kafka_conf` доводяться до рядків самі, тож `false` із YAML доїжджає до librdkafka як `"false"`, а не як порожнє місце.

Споживачеві потрібен `group.id`: без нього нема де тримати зсув, і транспорт скаже це замість падіння librdkafka. `enable.auto.commit` виставляється в `false`, якщо ви не задали своє, — з увімкненим автокомітом зсув рухає фоновий потік, і ні `ack`, ні `reject` уже нічого не вирішують.

## DSN

Усе налаштування вміщується в рядок підключення:

```
kafka+sasl+ssl://user:pass@b-1:9098,b-2:9098/orders.created?group.id=orders-service&flush_timeout=5000
```

| Частина | Що з неї стає |
|---|---|
| схема | `security.protocol` |
| `user:pass` | `sasl.username` і `sasl.password`, приймається лише зі схемою `kafka+sasl…` |
| хости через кому | список брокерів |
| шлях | топік |
| параметр **із крапкою** | властивість librdkafka, як у `kafka_conf` |
| параметр **без крапки** | опція транспорту, звіряється з тим самим переліком |

Написане в DSN перекриває масив `options` — так само, як у рідних транспортах Symfony.

| Схема | `security.protocol` |
|---|---|
| `kafka://` | `plaintext` |
| `kafka+ssl://` | `ssl` |
| `kafka+sasl://` | `sasl_plaintext` |
| `kafka+sasl+ssl://` | `sasl_ssl` |

Значення, задане в `kafka_conf`, завжди виграє над схемою. Мішанина схем в одному DSN — помилка: `security.protocol` один на весь клієнт, а не на брокер.

## Ключ партиції

Повідомлення з однаковим ключем лягають в одну партицію й зберігають порядок:

```php
$bus->dispatch(new OrderPlaced($orderId), [new KafkaKeyStamp($orderId)]);
```

## Пачки й нечитабельні повідомлення

Приймач віддає пачку щоразу, коли воркер її просить: перше повідомлення чекає `receive_timeout`, решта добираються без очікування — скільки вже лежить у буфері. Воркер Symfony навчився просити (`messenger:consume --fetch-size=10`) у версії 8.1, тож на 7.3 та 8.0 кожен виклик приносить одне повідомлення.

Повідомлення, яке серіалізатор не розбирає, повертається конвертом із `MessageDecodingFailedException` — саме такої форми `ReceiverInterface` вимагає від транспорту. Воркер проводить його звичайним шляхом retry та failure і робить `ack`, тож зсув рушає далі й одне побите повідомлення не спиняє партицію.

Що саме збереже failure transport, залежить від лінії Symfony. Починаючи з 8.1 її серіалізатори повертають цей виняток із вихідним закодованим конвертом усередині, тож вихідне тіло їде разом із ним у failure transport, і `messenger:failed:retry` розбирає його знову, щойно причину усунуто. На 7.3 та 8.0 серіалізатор натомість кидає виняток, носити payload йому нема де, і у failure transport лишається сам виняток.

## Як це влаштовано

`Kafka\Connection` — єдине місце, де код говорить із librdkafka: клієнти, підтвердження доставки, зсуви, ребаланс, вихід із групи. `Transport\KafkaSender` і `Transport\KafkaReceiver` знають лише мову Messenger — серіалізацію, штампи, ack і reject — і ділять одне з'єднання. `Kafka\Dsn` та `Kafka\Options` розбирають конфігурацію й не пускають далі те, чого не впізнали.

Транспорт реалізує `CloseableTransportInterface`: на зупинці воркера споживач одразу виходить із групи, і партиції переїжджають до сусідів негайно, а не за `session.timeout.ms`.

## [Інші бібліотеки UFO-Tech](https://packagist.org/packages/ufo-tech/)
Цей та інші сімнадцять пакетів, опублікованих під вендором [ufo-tech](https://packagist.org/packages/ufo-tech/) на Packagist.

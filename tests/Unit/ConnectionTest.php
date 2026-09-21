<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RdKafka\TopicPartition;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\TransportException;
use Ufo\KafkaMessenger\Kafka\Connection;
use Ufo\KafkaMessenger\Tests\Support\Doubles;
use Ufo\KafkaMessenger\Tests\Support\FakeConsumer;

#[CoversClass(Connection::class)]
final class ConnectionTest extends TestCase
{
    public function testMessageGoesToTheConfiguredTopic(): void
    {
        $clients = Doubles::clients();
        Doubles::connection($clients)->publish('payload', ['type' => 'stdClass'], null);

        self::assertSame('events', $clients->producer->topicName);
        self::assertSame('payload', $clients->producer->produced[0]['payload']);
        self::assertSame(['type' => 'stdClass'], $clients->producer->produced[0]['headers']);
    }

    public function testPartitionKeyReachesTheProducer(): void
    {
        $clients = Doubles::clients();
        Doubles::connection($clients)->publish('payload', [], 'ufo-tech');

        self::assertSame('ufo-tech', $clients->producer->produced[0]['key']);
    }

    public function testUndrainedQueueIsAFailure(): void
    {
        $clients = Doubles::clients();
        $clients->producer->flushCode = \RD_KAFKA_RESP_ERR__TIMED_OUT;

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('did not drain');

        Doubles::connection($clients)->publish('payload', [], null);
    }

    public function testFlushIsRetriedBeforeGivingUp(): void
    {
        $clients = Doubles::clients();
        $clients->producer->flushCodes = [\RD_KAFKA_RESP_ERR__TIMED_OUT, \RD_KAFKA_RESP_ERR_NO_ERROR];

        Doubles::connection($clients, ['flush_retries' => 1])->publish('payload', [], null);

        self::assertSame(2, $clients->producer->flushCalls);
    }

    public function testBrokerRejectionIsNotSilentlySwallowed(): void
    {
        $clients = Doubles::clients();
        $clients->producer->deliveryError = 'Broker: Message too large';

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Message too large');

        Doubles::connection($clients)->publish('payload', [], null);
    }

    public function testUnconfirmedDeliveryIsAFailure(): void
    {
        $clients = Doubles::clients();
        $clients->producer->deliverySilently = true;

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('confirmed no delivery');

        Doubles::connection($clients)->publish('payload', [], null);
    }

    public function testConsumerSubscribesToTheConfiguredTopicOnce(): void
    {
        $clients = Doubles::clients();
        $connection = Doubles::consuming($clients);

        $connection->consume(1);
        $connection->consume(1);

        self::assertSame(['events'], $clients->consumer?->subscribed);
    }

    public function testConsumerIsBuiltWithAutoCommitOff(): void
    {
        $clients = Doubles::clients();
        Doubles::consuming($clients)->consume(1);

        self::assertSame('false', $clients->consumer?->conf['enable.auto.commit'] ?? null);
    }

    public function testProducerIsSparedTheConsumerProperty(): void
    {
        $clients = Doubles::clients();
        Doubles::consuming($clients)->publish('payload', [], null);

        self::assertArrayNotHasKey('enable.auto.commit', $clients->producer->conf);
    }

    public function testConsumingWithoutGroupIdIsRefused(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('group.id');

        Doubles::connection(Doubles::clients())->consume(1);
    }

    public function testMessageIsHandedOverAsIs(): void
    {
        $clients = Doubles::clients(FakeConsumer::message('payload', 42));

        $messages = Doubles::consuming($clients)->consume(1);

        self::assertCount(1, $messages);
        self::assertSame(42, $messages[0]->offset);
    }

    #[DataProvider('quiet')]
    public function testQuietAnswersAreNotFailures(int $code): void
    {
        $clients = Doubles::clients(FakeConsumer::error($code));

        self::assertSame([], Doubles::consuming($clients)->consume(1));
    }

    /** @return iterable<string, array{int}> */
    public static function quiet(): iterable
    {
        yield 'partition eof' => [\RD_KAFKA_RESP_ERR__PARTITION_EOF];
        yield 'timed out' => [\RD_KAFKA_RESP_ERR__TIMED_OUT];
        yield 'transport failure' => [\RD_KAFKA_RESP_ERR__TRANSPORT];
    }

    public function testRealBrokerErrorIsRaised(): void
    {
        $clients = Doubles::clients(FakeConsumer::error(\RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, 'not authorized'));

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('not authorized');

        Doubles::consuming($clients)->consume(1);
    }

    public function testBatchStopsAtTheAskedSize(): void
    {
        $clients = Doubles::clients(
            FakeConsumer::message('one'),
            FakeConsumer::message('two'),
            FakeConsumer::message('three'),
        );

        self::assertCount(2, Doubles::consuming($clients)->consume(2));
        self::assertCount(1, $clients->consumer?->queue ?? []);
    }

    public function testBatchStopsWhenThePartitionRunsDry(): void
    {
        $clients = Doubles::clients(FakeConsumer::message('one'));

        self::assertCount(1, Doubles::consuming($clients)->consume(10));
    }

    public function testErrorOnTheTailDoesNotEatWhatWasAlreadyRead(): void
    {
        $clients = Doubles::clients(
            FakeConsumer::message('one'),
            FakeConsumer::error(\RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, 'not authorized'),
        );

        self::assertCount(1, Doubles::consuming($clients)->consume(5));
    }

    public function testAckCommitsTheOffset(): void
    {
        $clients = Doubles::clients(FakeConsumer::message('payload', 7));
        $connection = Doubles::consuming($clients);

        $connection->commit($connection->consume(1)[0]);

        self::assertCount(1, $clients->consumer?->committed ?? []);
        self::assertSame([], $clients->consumer?->committedAsync);
    }

    public function testAsyncCommitIsUsedWhenAsked(): void
    {
        $clients = Doubles::clients(FakeConsumer::message('payload'));
        $connection = Doubles::consuming($clients, ['commit_async' => true]);

        $connection->commit($connection->consume(1)[0]);

        self::assertCount(1, $clients->consumer?->committedAsync ?? []);
        self::assertSame([], $clients->consumer?->committed);
    }

    public function testCloseLeavesTheGroup(): void
    {
        $clients = Doubles::clients();
        $connection = Doubles::consuming($clients);
        $connection->consume(1);

        $connection->close();

        self::assertTrue($clients->consumer?->closed);
    }

    public function testConsumingAfterCloseSubscribesAnew(): void
    {
        $clients = Doubles::clients();
        $connection = Doubles::consuming($clients);
        $connection->consume(1);
        $connection->close();

        $clients->consumer = new FakeConsumer();
        $connection->consume(1);

        self::assertSame(['events'], $clients->consumer->subscribed);
    }

    public function testAssignedPartitionsAreTakenOver(): void
    {
        $clients = Doubles::clients();
        Doubles::consuming($clients)->consume(1);

        ($clients->onRebalance)($clients->consumer, \RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, [new TopicPartition('events', 3)]);

        self::assertCount(1, $clients->consumer?->assigned ?? []);
        self::assertSame(3, $clients->consumer?->assigned[0][0]->getPartition());
    }

    public function testRevokedPartitionsAreGivenBack(): void
    {
        $clients = Doubles::clients();
        Doubles::consuming($clients)->consume(1);

        ($clients->onRebalance)($clients->consumer, \RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS, [new TopicPartition('events', 3)]);

        self::assertSame([null], $clients->consumer?->assigned);
    }

    public function testFailedRebalanceIsRaised(): void
    {
        $clients = Doubles::clients();
        Doubles::consuming($clients)->consume(1);

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('rebalance failed');

        ($clients->onRebalance)($clients->consumer, \RD_KAFKA_RESP_ERR__FATAL);
    }
}

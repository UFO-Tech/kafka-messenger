<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Tests\Support;

use RdKafka\Message;

final class FakeMessage extends Message
{
    public string $reason = '';

    public function errstr(): string
    {
        return $this->reason;
    }
}

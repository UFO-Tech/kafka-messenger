<?php

declare(strict_types=1);

namespace Ufo\KafkaMessenger\Transport\Stamp;

use RdKafka\Message;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

readonly class KafkaMessageStamp implements NonSendableStampInterface
{
    public function __construct(
        public Message $message,
    ) {
    }
}

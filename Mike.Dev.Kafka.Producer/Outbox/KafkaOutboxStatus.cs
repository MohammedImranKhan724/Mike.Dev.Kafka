namespace Mike.Dev.Kafka.Producer.Outbox;

public enum KafkaOutboxStatus
{
    Pending = 0,
    Publishing = 1,
    Published = 2,
    Failed = 3,
    DeadLettered = 4
}
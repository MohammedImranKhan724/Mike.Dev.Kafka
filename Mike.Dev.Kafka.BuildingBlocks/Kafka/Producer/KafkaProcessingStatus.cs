namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;

public enum KafkaProcessingStatus
{
    Success,
    Retry,
    DeadLetter
}
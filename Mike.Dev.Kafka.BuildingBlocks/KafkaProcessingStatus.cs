namespace Mike.Dev.Kafka.BuildingBlocks;

public enum KafkaProcessingStatus
{
    Success,
    Retry,
    DeadLetter
}
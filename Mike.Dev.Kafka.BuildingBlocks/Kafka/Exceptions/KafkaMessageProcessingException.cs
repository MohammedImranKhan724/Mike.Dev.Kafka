namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Exceptions;

public sealed class KafkaMessageProcessingException : Exception
{
    public KafkaMessageProcessingException(
        string message,
        Exception innerException)
        : base(message, innerException)
    {
    }
}

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Exceptions;

public sealed class KafkaRetryExhaustedException : Exception
{
    public KafkaRetryExhaustedException(
        string message,
        Exception? innerException = null)
        : base(message, innerException)
    {
    }
}
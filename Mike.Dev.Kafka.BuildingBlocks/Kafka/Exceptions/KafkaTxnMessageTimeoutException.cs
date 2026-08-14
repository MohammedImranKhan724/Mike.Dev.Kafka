namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Exceptions;

public sealed class KafkaTxnMessageTimeoutException : Exception
{
    public KafkaTxnMessageTimeoutException(
        string message,
        Exception? innerException = null)
        : base(message, innerException)
    {
    }
}
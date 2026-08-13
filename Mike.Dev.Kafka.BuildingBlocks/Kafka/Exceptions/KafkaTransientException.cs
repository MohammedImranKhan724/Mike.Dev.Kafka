namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Exceptions;

public sealed class KafkaTransientException : Exception
{
    public KafkaTransientException(string message, Exception? innerException = null) : base(message, innerException)
    {

    }
}

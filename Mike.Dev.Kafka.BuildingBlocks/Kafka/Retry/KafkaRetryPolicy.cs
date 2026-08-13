using Mike.Dev.Kafka.BuildingBlocks.Kafka.Exceptions;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Retry;

public sealed class KafkaRetryPolicy
{
    public bool ShouldRetry(Exception exception)
    {
        return exception switch
        {
            KafkaTransientException => true,

            TimeoutException => true,

            HttpRequestException => true,

            _ => false
        };
    }
}
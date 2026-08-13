namespace Mike.Dev.Kafka.Consumer.Idempotency;

public interface IProcessedEventStore
{
    Task<bool> HasProcessedAsync(string eventId, CancellationToken cancellationToken);

    Task MarkProcessedAsync(string eventId, CancellationToken cancellationToken);
}

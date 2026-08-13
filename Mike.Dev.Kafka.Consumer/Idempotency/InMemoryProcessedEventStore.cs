namespace Mike.Dev.Kafka.Consumer.Idempotency;

public sealed class InMemoryProcessedEventStore
    : IProcessedEventStore
{
    private readonly HashSet<string> _processedEvents = new();

    private readonly object _lock = new();

    public Task<bool> HasProcessedAsync(
        string eventId,
        CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            return Task.FromResult(
                _processedEvents.Contains(eventId));
        }
    }

    public Task MarkProcessedAsync(
        string eventId,
        CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            _processedEvents.Add(eventId);
        }

        return Task.CompletedTask;
    }
}
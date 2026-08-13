namespace Mike.Dev.Kafka.Contracts.Events;

public sealed record DeviceEvent
{
    public required string EventId { get; init; }

    public required string CorrelationId { get; init; }

    public required long DeviceId { get; init; }

    public required string EventType { get; init; }

    public required string Message { get; init; }

    public DateTime TimestampUtc { get; init; }
}

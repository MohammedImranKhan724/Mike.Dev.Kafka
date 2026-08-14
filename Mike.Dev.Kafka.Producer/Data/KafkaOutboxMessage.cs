using Mike.Dev.Kafka.Producer.Outbox;

namespace Mike.Dev.Kafka.Producer.Data;

public sealed class KafkaOutboxMessage
{
    public long Id { get; set; }

    public Guid EventId { get; set; }

    public string Topic { get; set; } = string.Empty;

    public string Key { get; set; } = string.Empty;

    public string Payload { get; set; } = string.Empty;

    public string? Headers { get; set; }

    public KafkaOutboxStatus Status { get; set; }

    public int RetryCount { get; set; }

    public string? LastError { get; set; }

    public DateTime CreatedAtUtc { get; set; }

    public DateTime? ProcessingStartedAtUtc { get; set; }

    public DateTime? NextAttemptAtUtc { get; set; }

    public DateTime? PublishedAtUtc { get; set; }

    public DateTime UpdatedAtUtc { get; set; }
}
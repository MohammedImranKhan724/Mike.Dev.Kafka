using System.ComponentModel.DataAnnotations;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Retry;

public sealed class KafkaRetryOptions
{
    public const string Section = "Kafka:Retry";

    public bool Enabled { get; set; } = true;

    [Range(1, 100)]
    public int MaxAttempts { get; set; } = 3;

    [Range(0, 300000)]
    public int InitialDelayMs { get; set; } = 1000;

    [Range(0, 3600000)]
    public int MaxDelayMs { get; set; } = 30000;

    [Range(1.0, 10.0)]
    public double BackoffMultiplier { get; set; } = 2.0;
}
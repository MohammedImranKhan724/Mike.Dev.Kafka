namespace Mike.Dev.Kafka.Producer.Options;

public sealed class KafkaOutboxDispatcherOptions
{
    public const string Section =
        "Kafka:Outbox";

    public int BatchSize { get; set; } = 100;

    public int PollIntervalMs { get; set; } = 1000;

    public int ErrorDelayMs { get; set; } = 5000;

    public int LeaseDurationSeconds { get; set; } = 60;

    public int InitialRetryDelayMs { get; set; } = 1000;

    public double BackoffMultiplier { get; set; } = 2;

    public int MaxRetryDelayMs { get; set; } = 30000;

    public int MaxRetryCount { get; set; } = 10;
}

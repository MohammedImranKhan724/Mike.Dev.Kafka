using System.ComponentModel.DataAnnotations;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;

public sealed class KafkaProducerOptions
{
    public const string Section = "Kafka:Producer";

    [Required]
    public string BootstrapServers { get; set; } = string.Empty;

    [Range(1, 600000)]
    public int MessageTimeoutMs { get; set; } = 30000;

    [Range(1, 600000)]
    public int RequestTimeoutMs { get; set; } = 30000;

    [Range(0, 100)]
    public int MessageSendMaxRetries { get; set; } = 5;

    [Range(0, 60000)]
    public int RetryBackoffMs { get; set; } = 100;

    public bool EnableIdempotence { get; set; } = true;

    [Required]
    public string Acks { get; set; } = "All";

    [Required]
    public string CompressionType { get; set; } = "Snappy";

    [Range(1, 104857600)]
    public int BatchSize { get; set; } = 16384;

    [Range(0, 60000)]
    public int LingerMs { get; set; } = 5;

    [Range(1, int.MaxValue)]
    public int QueueBufferingMaxMessages { get; set; } = 100000;

    [Range(1, int.MaxValue)]
    public int QueueBufferingMaxKbytes { get; set; } = 1048576;

    public KafkaTopics Topics { get; set; } = new();
}

public sealed class KafkaTopics
{
    [Required]
    public string DeviceEvents { get; set; } =
        "device-events";
}
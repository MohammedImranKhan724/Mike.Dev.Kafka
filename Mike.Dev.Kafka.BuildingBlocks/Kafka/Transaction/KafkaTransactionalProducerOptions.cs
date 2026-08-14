using System.ComponentModel.DataAnnotations;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Transaction;

public sealed class KafkaTransactionalProducerOptions
{
    public const string Section =
        "Kafka:Transaction";

    [Required]
    public string BootstrapServers { get; set; }
        = string.Empty;

    [Required]
    public string TransactionalIdPrefix { get; set; }
        = string.Empty;

    [Range(1000, 900000)]
    public int TransactionTimeoutMs { get; set; }
        = 60000;

    [Range(1000, 900000)]
    public int MessageTimeoutMs { get; set; }
        = 30000;

    [Range(1, 20)]
    public int MaxTransactionRetryAttempts { get; set; }
        = 3;

    [Range(1, 60000)]
    public int InitialRetryDelayMs { get; set; }
        = 1000;

    [Range(1, 10)]
    public double RetryBackoffMultiplier { get; set; }
        = 2;

    [Range(1, 120000)]
    public int MaxRetryDelayMs { get; set; }
        = 10000;

    public KafkaTransactionTopics Topics { get; set; }
        = new();
}

public sealed class KafkaTransactionTopics
{
    [Required]
    public string DeviceEvents { get; set; }
        = "device-events";

    [Required]
    public string DeviceEventsAudit { get; set; }
        = "device-events.audit";
}
using System.ComponentModel.DataAnnotations;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;

public sealed class KafkaConsumerOptions
{
    public const string Section = "Kafka:Consumer";

    [Required]
    public string BootstrapServers { get; set; } = string.Empty;

    [Required]
    public string GroupId { get; set; } = string.Empty;

    [Required]
    public string Topic { get; set; } = string.Empty;

    public string AutoOffsetReset { get; set; } = "Earliest";

    public bool EnableAutoCommit { get; set; } = false;

    public bool EnableAutoOffsetStore { get; set; } = false;

    [Range(1, 600000)]
    public int SessionTimeoutMs { get; set; } = 45000;

    [Range(1, 600000)]
    public int MaxPollIntervalMs { get; set; } = 300000;

    [Range(1, 1000000)]
    public int MaxPollRecords { get; set; } = 500;

    public string PartitionAssignmentStrategy { get; set; } = "CooperativeSticky";
}
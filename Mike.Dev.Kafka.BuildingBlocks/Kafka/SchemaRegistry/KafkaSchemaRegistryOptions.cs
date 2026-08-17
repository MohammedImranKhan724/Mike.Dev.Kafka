using System.ComponentModel.DataAnnotations;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.SchemaRegistry;

public sealed class KafkaSchemaRegistryOptions
{
    public const string Section = "Kafka:SchemaRegistry";

    [Required]
    public string Url { get; set; } = string.Empty;

    public bool AutoRegisterSchemas { get; set; } = true;

    public bool UseLatestVersion { get; set; } = false;
}

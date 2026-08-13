namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.DeadLetter;

public sealed class KafkaDeadLetterOptions
{
    public const string Section = "Kafka:DeadLetter";

    public bool Enabled { get; set; } = true;

    public string TopicSuffix { get; set; } = ".DLT";
}
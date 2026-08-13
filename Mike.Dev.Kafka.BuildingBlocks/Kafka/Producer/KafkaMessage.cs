namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;

public sealed class KafkaMessage<TKey, TValue>
{
    public required string Topic { get; init; }

    public TKey? Key { get; init; }

    public required TValue Value { get; init; }

    public IDictionary<string, string>? Headers { get; init; }
}

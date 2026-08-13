using Confluent.Kafka;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;

public sealed class KafkaConsumedMessage<TKey, TValue>
{
    public required string Topic { get; init; }

    public required Partition Partition { get; init; }

    public required Offset Offset { get; init; }

    public TKey? Key { get; init; }

    public required TValue Value { get; init; }

    public Headers? Headers { get; init; }

    public required ConsumeResult<TKey, TValue> RawResult { get; init; }
}
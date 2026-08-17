using Confluent.Kafka;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Serialization;

public sealed class KafkaAsyncDeserializerAdapter<T> : IAsyncDeserializer<T>
{
    private readonly KafkaJsonDeserializer<T> _inner;

    public KafkaAsyncDeserializerAdapter(KafkaJsonDeserializer<T>? inner = null)
    {
        _inner = inner ?? new KafkaJsonDeserializer<T>();
    }

    public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
    {
        return Task.FromResult(_inner.Deserialize(data.Span, isNull, context));
    }
}
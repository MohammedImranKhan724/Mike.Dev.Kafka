using Confluent.Kafka;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Serialization;

public sealed class KafkaAsyncSerializerAdapter<T> : IAsyncSerializer<T>
{
    private readonly KafkaJsonSerializer<T> _inner;

    public KafkaAsyncSerializerAdapter(KafkaJsonSerializer<T>? inner = null)
    {
        _inner = inner ?? new KafkaJsonSerializer<T>();
    }

    public Task<byte[]> SerializeAsync(T data, SerializationContext context)
    {
        return Task.FromResult(_inner.Serialize(data, context));
    }
}

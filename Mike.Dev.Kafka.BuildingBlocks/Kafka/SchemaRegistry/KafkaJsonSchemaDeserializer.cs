using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.SchemaRegistry;

public sealed class KafkaJsonSchemaDeserializer<T> : IAsyncDeserializer<T>
    where T : class
{
    private readonly JsonDeserializer<T> _deserializer;

    public KafkaJsonSchemaDeserializer(
        ISchemaRegistryClient schemaRegistryClient)
    {
        _deserializer = new JsonDeserializer<T>();
    }

    public Task<T> DeserializeAsync(
        ReadOnlyMemory<byte> data,
        bool isNull,
        SerializationContext context)
    {
        return _deserializer.DeserializeAsync(
            data,
            isNull,
            context);
    }
}
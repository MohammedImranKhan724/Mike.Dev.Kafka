using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.SchemaRegistry;

public sealed class KafkaJsonSchemaSerializer<T> : IAsyncSerializer<T>
    where T : class
{
    private readonly JsonSerializer<T> _serializer;

    public KafkaJsonSchemaSerializer(
        ISchemaRegistryClient schemaRegistryClient,
        bool autoRegisterSchema = true)
    {
        var config = new JsonSerializerConfig
        {
            AutoRegisterSchemas = autoRegisterSchema
        };

        _serializer = new JsonSerializer<T>(
            schemaRegistryClient,
            config);
    }

    public Task<byte[]> SerializeAsync(
        T data,
        SerializationContext context)
    {
        return _serializer.SerializeAsync(
            data,
            context);
    }
}
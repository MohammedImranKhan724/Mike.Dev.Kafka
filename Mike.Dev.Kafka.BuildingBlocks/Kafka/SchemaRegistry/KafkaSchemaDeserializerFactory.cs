using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Options;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.SchemaRegistry;

public sealed class KafkaSchemaDeserializerFactory
    : IKafkaSchemaDeserializerFactory
{
    private readonly ISchemaRegistryClient _client;
    private readonly KafkaSchemaRegistryOptions _options;

    public KafkaSchemaDeserializerFactory(
        ISchemaRegistryClient client,
        IOptions<KafkaSchemaRegistryOptions> options)
    {
        _client = client;
        _options = options.Value;
    }

    public JsonDeserializer<T> CreateDeserializer<T>() where T : class
    {
        var config = new JsonDeserializerConfig
        {
            UseLatestVersion = _options.UseLatestVersion
        };

        return new JsonDeserializer<T>(
            _client,
            config);
    }
}
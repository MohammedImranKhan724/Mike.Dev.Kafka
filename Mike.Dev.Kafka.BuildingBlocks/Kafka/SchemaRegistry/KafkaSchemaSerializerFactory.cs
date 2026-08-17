using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Options;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.SchemaRegistry;

public sealed class KafkaSchemaSerializerFactory
    : IKafkaSchemaSerializerFactory
{
    private readonly ISchemaRegistryClient _client;
    private readonly KafkaSchemaRegistryOptions _options;

    public KafkaSchemaSerializerFactory(
        ISchemaRegistryClient client,
        IOptions<KafkaSchemaRegistryOptions> options)
    {
        _client = client;
        _options = options.Value;
    }

    public JsonSerializer<T> CreateSerializer<T>() where T : class
    {
        var config = new JsonSerializerConfig
        {
            AutoRegisterSchemas = _options.AutoRegisterSchemas,
            UseLatestVersion = _options.UseLatestVersion
        };

        return new JsonSerializer<T>(
            _client,
            config);
    }
}
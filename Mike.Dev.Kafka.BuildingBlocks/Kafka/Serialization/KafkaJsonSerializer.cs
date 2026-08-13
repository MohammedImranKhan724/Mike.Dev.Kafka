using Confluent.Kafka;
using System.Text.Json;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Serialization;

public sealed class KafkaJsonSerializer<T>
    : ISerializer<T>
{
    private readonly JsonSerializerOptions _options;

    public KafkaJsonSerializer(
        JsonSerializerOptions? options = null)
    {
        _options = options ??
            new JsonSerializerOptions(
                JsonSerializerDefaults.Web);
    }

    public byte[] Serialize(
        T data,
        SerializationContext context)
    {
        return JsonSerializer.SerializeToUtf8Bytes(
            data,
            _options);
    }
}
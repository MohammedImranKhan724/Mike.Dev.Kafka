using Confluent.Kafka;
using System.Text.Json;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Serialization;

public sealed class KafkaJsonDeserializer<T>
    : IDeserializer<T>
{
    private readonly JsonSerializerOptions _options;

    public KafkaJsonDeserializer(
        JsonSerializerOptions? options = null)
    {
        _options = options ??
            new JsonSerializerOptions(
                JsonSerializerDefaults.Web);
    }

    public T Deserialize(
        ReadOnlySpan<byte> data,
        bool isNull,
        SerializationContext context)
    {
        if (isNull)
        {
            throw new InvalidOperationException(
                $"Kafka message value for type " +
                $"{typeof(T).Name} is null.");
        }

        var value =
            JsonSerializer.Deserialize<T>(
                data,
                _options);

        if (value is null)
        {
            throw new InvalidOperationException(
                $"Unable to deserialize Kafka message " +
                $"to {typeof(T).Name}.");
        }

        return value;
    }
}
using Confluent.Kafka;
using System.Text;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Serialization;

public sealed class KafkaStringDeserializer
    : IDeserializer<string>
{
    public string Deserialize(
        ReadOnlySpan<byte> data,
        bool isNull,
        SerializationContext context)
    {
        if (isNull)
        {
            throw new InvalidOperationException(
                "Kafka string value is null.");
        }

        return Encoding.UTF8.GetString(data);
    }
}
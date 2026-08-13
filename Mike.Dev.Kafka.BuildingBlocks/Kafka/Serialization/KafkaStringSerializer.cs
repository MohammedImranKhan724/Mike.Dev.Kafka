using Confluent.Kafka;
using System.Text;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Serialization;

public sealed class KafkaStringSerializer : ISerializer<string>
{
    public byte[] Serialize(string data, SerializationContext context)
    {
        return Encoding.UTF8.GetBytes(data);
    }
}
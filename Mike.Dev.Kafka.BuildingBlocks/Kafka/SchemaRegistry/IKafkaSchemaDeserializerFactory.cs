using Confluent.SchemaRegistry.Serdes;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.SchemaRegistry;

public interface IKafkaSchemaDeserializerFactory
{
    JsonDeserializer<T> CreateDeserializer<T>() where T : class;
}

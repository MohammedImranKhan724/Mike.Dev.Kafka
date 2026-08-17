using Confluent.SchemaRegistry.Serdes;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.SchemaRegistry;

public interface IKafkaSchemaSerializerFactory
{
    JsonSerializer<T> CreateSerializer<T>() where T : class;
}
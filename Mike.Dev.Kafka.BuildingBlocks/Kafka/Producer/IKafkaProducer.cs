using Confluent.Kafka;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;

public interface IKafkaProducer<TKey, TValue>
{
    Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        KafkaMessage<TKey, TValue> message,
        CancellationToken cancellationToken = default);
}
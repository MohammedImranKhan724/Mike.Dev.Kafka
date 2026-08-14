using Confluent.Kafka;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Transaction;

public interface IKafkaTransactionalProducer<TKey, TValue>
{
    Task ProduceAndCommitAsync(
        KafkaMessage<TKey, TValue> message,
        IEnumerable<TopicPartitionOffset> offsetsToCommit,
        IConsumerGroupMetadata consumerGroupMetadata,
        CancellationToken cancellationToken = default);

    Task ProduceManyAsync(
        IEnumerable<KafkaMessage<TKey, TValue>> messages,
        CancellationToken cancellationToken = default);
}
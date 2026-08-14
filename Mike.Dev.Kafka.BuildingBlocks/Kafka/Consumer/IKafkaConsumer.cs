using Confluent.Kafka;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;

public interface IKafkaConsumer<TKey, TValue>
{
    Task ConsumeAsync(
        Func<
            KafkaConsumedMessage<TKey, TValue>,
            CancellationToken,
            Task<KafkaProcessingStatus>> handler,
        CancellationToken cancellationToken);

    void Commit(
        KafkaConsumedMessage<TKey, TValue> message);

    IConsumerGroupMetadata GetGroupMetadata();
}
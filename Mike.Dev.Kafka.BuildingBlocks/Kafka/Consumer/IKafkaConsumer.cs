using Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;

public interface IKafkaConsumer<TKey, TValue>
{
    Task ConsumeAsync(Func<KafkaConsumedMessage<TKey, TValue>, CancellationToken, Task<KafkaProcessingStatus>> handler, CancellationToken cancellationToken);
}
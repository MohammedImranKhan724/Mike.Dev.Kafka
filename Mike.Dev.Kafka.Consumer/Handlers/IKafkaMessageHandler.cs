using Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;

namespace Mike.Dev.Kafka.Consumer.Handlers;

public interface IKafkaMessageHandler<TKey, TValue>
{
    Task HandleAsync(
        KafkaConsumedMessage<TKey, TValue> message,
        CancellationToken cancellationToken);
}
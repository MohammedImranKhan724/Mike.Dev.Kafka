using Mike.Dev.Kafka.Contracts.Events;
using Mike.Dev.Kafka.Producer.Data;

namespace Mike.Dev.Kafka.Producer.Outbox;

public interface IKafkaOutboxMessageFactory
{
    KafkaOutboxMessage CreateDeviceEventMessage(
        DeviceEvent deviceEvent,
        string topic);
}
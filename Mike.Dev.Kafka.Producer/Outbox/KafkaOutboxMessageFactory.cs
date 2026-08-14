using Mike.Dev.Kafka.Contracts.Events;
using Mike.Dev.Kafka.Producer.Data;
using System.Text.Json;

namespace Mike.Dev.Kafka.Producer.Outbox;

public sealed class KafkaOutboxMessageFactory
    : IKafkaOutboxMessageFactory
{
    private readonly JsonSerializerOptions _jsonOptions;

    public KafkaOutboxMessageFactory()
    {
        _jsonOptions =
            new JsonSerializerOptions(
                JsonSerializerDefaults.Web);
    }

    public KafkaOutboxMessage CreateDeviceEventMessage(
        DeviceEvent deviceEvent,
        string topic)
    {
        ArgumentNullException.ThrowIfNull(deviceEvent);

        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException(
                "Kafka topic is required.",
                nameof(topic));
        }

        var headers = new Dictionary<string, string>
        {
            ["event-id"] =
                deviceEvent.EventId,

            ["event-type"] =
                deviceEvent.EventType,

            ["correlation-id"] =
                deviceEvent.CorrelationId,

            ["source"] =
                "Mike.Dev.Kafka.Producer",

            ["schema-version"] =
                "1"
        };

        return new KafkaOutboxMessage
        {
            EventId =
                Guid.Parse(deviceEvent.EventId),

            Topic =
                topic,

            Key =
                deviceEvent.DeviceId.ToString(),

            Payload =
                JsonSerializer.Serialize(
                    deviceEvent,
                    _jsonOptions),

            Headers =
                JsonSerializer.Serialize(
                    headers,
                    _jsonOptions),

            Status =
                KafkaOutboxStatus.Pending,

            RetryCount = 0,

            CreatedAtUtc =
                DateTime.UtcNow,

            UpdatedAtUtc =
                DateTime.UtcNow
        };
    }
}
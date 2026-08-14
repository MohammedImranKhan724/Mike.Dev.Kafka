using Microsoft.Extensions.Options;
using Mike.Dev.Kafka.BuildingBlocks.Kafka;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Transaction;
using Mike.Dev.Kafka.Contracts.Events;

namespace Mike.Dev.Kafka.Producer.Services;

public sealed class DeviceEventTransactionalProducer
{
    private readonly IKafkaTransactionalProducer<string, DeviceEvent> _producer;

    private readonly ILogger<DeviceEventTransactionalProducer> _logger;

    private readonly KafkaTransactionalProducerOptions _options;

    public DeviceEventTransactionalProducer(
        IKafkaTransactionalProducer<string, DeviceEvent> producer,
        IOptions<KafkaTransactionalProducerOptions> options,
        ILogger<DeviceEventTransactionalProducer> logger)
    {
        _producer = producer;
        _options = options.Value;
        _logger = logger;
    }

    public async Task ProduceAtomicallyAsync(DeviceEvent deviceEvent, CancellationToken cancellationToken = default)
    {
        var primaryTopic = _options.Topics.DeviceEvents;

        var auditTopic = _options.Topics.DeviceEventsAudit;

        var headers =
            new Dictionary<string, string>
            {
                ["event-id"] = deviceEvent.EventId,

                ["event-type"] = deviceEvent.EventType,

                ["correlation-id"] = deviceEvent.CorrelationId,

                ["source"] = "Mike.Dev.Kafka.Producer",

                ["schema-version"] = "1"
            };

        var messages =
            new[]
            {
                new KafkaMessage<string, DeviceEvent>
                {
                    Topic = primaryTopic,
                    Key = deviceEvent.DeviceId.ToString(),
                    Value = deviceEvent,
                    Headers = headers
                },

                new KafkaMessage<string, DeviceEvent>
                {
                    Topic = auditTopic,
                    Key = deviceEvent.DeviceId.ToString(),
                    Value = deviceEvent,
                    Headers = headers
                }
            };

        await _producer.ProduceManyAsync(
            messages,
            cancellationToken);

        _logger.LogInformation(
            "Device event published atomically. " +
            "EventId={EventId}, " +
            "PrimaryTopic={PrimaryTopic}, " +
            "AuditTopic={AuditTopic}",
            deviceEvent.EventId,
            primaryTopic,
            auditTopic);
    }
}
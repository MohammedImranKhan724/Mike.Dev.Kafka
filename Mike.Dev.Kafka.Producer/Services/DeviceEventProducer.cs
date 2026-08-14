using Microsoft.Extensions.Options;
using Mike.Dev.Kafka.BuildingBlocks.Kafka;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;
using Mike.Dev.Kafka.Contracts.Events;

namespace Mike.Dev.Kafka.Producer.Services;

public sealed class DeviceEventProducer
{
    private readonly IKafkaProducer<string, DeviceEvent> _producer;
    private readonly ILogger<DeviceEventProducer> _logger;

    private readonly KafkaProducerOptions _options;

    public DeviceEventProducer(
        IKafkaProducer<string, DeviceEvent> producer,
        IOptions<KafkaProducerOptions> options,
        ILogger<DeviceEventProducer> logger)
    {
        _producer = producer;
        _options = options.Value;
        _logger = logger;
    }

    public async Task ProduceAsync(
        DeviceEvent deviceEvent,
        CancellationToken cancellationToken = default)
    {
        var message = new KafkaMessage<string, DeviceEvent>
        {
            Topic = _options.Topics.DeviceEvents,

            Key = deviceEvent.DeviceId.ToString(),

            Value = deviceEvent,

            Headers = new Dictionary<string, string>
            {
                ["event-id"] = deviceEvent.EventId,
                ["event-type"] = deviceEvent.EventType,
                ["correlation-id"] = deviceEvent.CorrelationId,
                ["source"] = "Mike.Dev.Kafka.Producer",
                ["schema-version"] = "1"
            }
        };

        var result =
            await _producer.ProduceAsync(
                message,
                cancellationToken);

        _logger.LogInformation(
            "Device event published. " +
            "EventId={EventId}, " +
            "DeviceId={DeviceId}, " +
            "Topic={Topic}, " +
            "Partition={Partition}, " +
            "Offset={Offset}",
            deviceEvent.EventId,
            deviceEvent.DeviceId,
            result.Topic,
            result.Partition,
            result.Offset);
    }
}
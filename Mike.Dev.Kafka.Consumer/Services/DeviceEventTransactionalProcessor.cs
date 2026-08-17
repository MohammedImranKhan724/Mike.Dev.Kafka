using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Mike.Dev.Kafka.BuildingBlocks.Kafka;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Transaction;
using Mike.Dev.Kafka.Contracts.Events;

namespace Mike.Dev.Kafka.Consumer.Services;

public sealed class DeviceEventTransactionalProcessor
{
    private readonly
        IKafkaTransactionalProducer<string, DeviceEvent>
        _producer;

    private readonly
        KafkaTransactionalProducerOptions
        _options;

    private readonly
        ILogger<DeviceEventTransactionalProcessor>
        _logger;

    public DeviceEventTransactionalProcessor(
        IKafkaTransactionalProducer<string, DeviceEvent> producer,
        IOptions<KafkaTransactionalProducerOptions> options,
        ILogger<DeviceEventTransactionalProcessor> logger)
    {
        _producer = producer;
        _options = options.Value;
        _logger = logger;
    }

    public async Task ProcessAsync(
        KafkaConsumedMessage<string, DeviceEvent> message,
        IConsumerGroupMetadata groupMetadata,
        CancellationToken cancellationToken)
    {
        var input =
            message.Value;

        var output =
            new DeviceEvent
            {
                EventId =
                    Guid.NewGuid().ToString("N"),

                CorrelationId =
                    input.CorrelationId,

                DeviceId =
                    input.DeviceId,

                EventType =
                    "Processed",

                Message =
                    $"Processed device {input.DeviceId}",

                TimestampUtc =
                    DateTime.UtcNow
            };

        var outputMessage =
            new KafkaMessage<string, DeviceEvent>
            {
                Topic =
                    _options.Topics.DeviceEventsAudit,

                Key =
                    input.DeviceId.ToString(),

                Value =
                    output,

                Headers =
                    new Dictionary<string, string>
                    {
                        ["event-id"] =
                            output.EventId,

                        ["event-type"] =
                            output.EventType,

                        ["correlation-id"] =
                            output.CorrelationId,

                        ["source"] =
                            "Mike.Dev.Kafka.Consumer",

                        ["schema-version"] =
                            "1"
                    }
            };

        var offsetToCommit =
            message.NextOffset;

        await _producer.ProduceAndCommitAsync(
            outputMessage,
            new[]
            {
                offsetToCommit
            },
            groupMetadata,
            cancellationToken);

        _logger.LogInformation(
            "EOS processing completed. " +
            "Input={Topic}-{Partition}-{Offset}, " +
            "OutputTopic={OutputTopic}",
            message.Topic,
            message.Partition,
            message.Offset,
            outputMessage.Topic);
    }
}
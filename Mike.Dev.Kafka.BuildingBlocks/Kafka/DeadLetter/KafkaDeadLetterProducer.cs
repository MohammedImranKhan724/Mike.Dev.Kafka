using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.DeadLetter;

public sealed class KafkaDeadLetterProducer<TKey, TValue>
{
    private readonly IKafkaProducer<TKey, TValue> _producer;
    private readonly KafkaDeadLetterOptions _options;
    private readonly ILogger<KafkaDeadLetterProducer<TKey, TValue>> _logger;

    public KafkaDeadLetterProducer(
        IKafkaProducer<TKey, TValue> producer,
        IOptions<KafkaDeadLetterOptions> options,
        ILogger<KafkaDeadLetterProducer<TKey, TValue>> logger)
    {
        _producer = producer;
        _options = options.Value;
        _logger = logger;
    }

    public async Task PublishAsync(
        ConsumeResult<TKey, TValue> result,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogWarning(
                exception,
                "Kafka DLT publishing is disabled. " +
                "Topic={Topic}, Partition={Partition}, Offset={Offset}",
                result.Topic,
                result.Partition,
                result.Offset);

            return;
        }

        var dltTopic =
            result.Topic + _options.TopicSuffix;

        var headers = new Dictionary<string, string>
        {
            ["x-original-topic"] =
                result.Topic,

            ["x-original-partition"] =
                result.Partition.Value.ToString(),

            ["x-original-offset"] =
                result.Offset.Value.ToString(),

            ["x-exception-type"] =
                exception.GetType().FullName ?? "Unknown",

            ["x-exception-message"] =
                exception.Message
        };

        var kafkaMessage =
            new KafkaMessage<TKey, TValue>
            {
                Topic = dltTopic,
                Key = result.Message.Key,
                Value = result.Message.Value,
                Headers = headers
            };

        await _producer.ProduceAsync(
            kafkaMessage,
            cancellationToken);

        _logger.LogError(
            exception,
            "Message published to Kafka DLT. " +
            "Topic={Topic}, " +
            "Partition={Partition}, " +
            "Offset={Offset}, " +
            "DLT={DltTopic}",
            result.Topic,
            result.Partition,
            result.Offset,
            dltTopic);
    }
}
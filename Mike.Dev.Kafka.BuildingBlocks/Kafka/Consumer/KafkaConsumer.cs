using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;

public sealed class KafkaConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>, IDisposable
{
    private readonly IConsumer<TKey, TValue> _consumer;

    private readonly ILogger<KafkaConsumer<TKey, TValue>> _logger;

    public KafkaConsumer(
        IOptions<KafkaConsumerOptions> options,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        ILogger<KafkaConsumer<TKey, TValue>> logger)
    {
        _logger = logger;

        var settings = options.Value;

        var config = new ConsumerConfig
        {
            BootstrapServers =
                settings.BootstrapServers,

            GroupId =
                settings.GroupId,

            AutoOffsetReset =
                ParseAutoOffsetReset(
                    settings.AutoOffsetReset),

            EnableAutoCommit =
                settings.EnableAutoCommit,

            EnableAutoOffsetStore =
                settings.EnableAutoOffsetStore,

            SessionTimeoutMs =
                settings.SessionTimeoutMs,

            MaxPollIntervalMs =
                settings.MaxPollIntervalMs,

            MaxPollRecords =
                settings.MaxPollRecords,

            PartitionAssignmentStrategy = ParsePartitionAssignmentStrategy(settings.PartitionAssignmentStrategy),
        };

        _consumer = new ConsumerBuilder<TKey, TValue>(config)
            .SetKeyDeserializer(keyDeserializer)
            .SetValueDeserializer(valueDeserializer)
            .SetErrorHandler(OnError)
            .SetPartitionsAssignedHandler(OnPartitionsAssigned)
            .SetPartitionsRevokedHandler(OnPartitionsRevoked)
            .SetPartitionsLostHandler(OnPartitionsLost)
            .Build();

        _consumer.Subscribe(settings.Topic);

        _logger.LogInformation(
            "Kafka consumer created. " +
            "Topic={Topic}, GroupId={GroupId}",
            settings.Topic,
            settings.GroupId);
    }

    public async Task ConsumeAsync(
        Func<
            KafkaConsumedMessage<TKey, TValue>,
            CancellationToken,
            Task<KafkaProcessingStatus>> handler,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result =
                    _consumer.Consume(
                        cancellationToken);

                if (result is null)
                    continue;

                var message =
                    new KafkaConsumedMessage<TKey, TValue>
                    {
                        Topic = result.Topic,

                        Partition =
                            result.Partition,

                        Offset =
                            result.Offset,

                        Key =
                            result.Message.Key,

                        Value =
                            result.Message.Value,

                        Headers =
                            result.Message.Headers,

                        RawResult =
                            result
                    };
                var status = await handler(
                    message,
                    cancellationToken);

                switch (status)
                {
                    case KafkaProcessingStatus.Success:
                    case KafkaProcessingStatus.DeadLetter:

                        _consumer.Commit(result);

                        _logger.LogDebug(
                            "Kafka message committed. " +
                            "Topic={Topic}, " +
                            "Partition={Partition}, " +
                            "Offset={Offset}, " +
                            "Status={Status}",
                            result.Topic,
                            result.Partition,
                            result.Offset,
                            status);

                        break;

                    case KafkaProcessingStatus.Retry:

                        _logger.LogWarning(
                            "Kafka message was not committed. " +
                            "Message will be redelivered. " +
                            "Topic={Topic}, " +
                            "Partition={Partition}, " +
                            "Offset={Offset}",
                            result.Topic,
                            result.Partition,
                            result.Offset);

                        break;

                    default:

                        throw new InvalidOperationException(
                            $"Unknown Kafka processing status: {status}");
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(
                    ex,
                    "Kafka consume error. " +
                    "Reason={Reason}",
                    ex.Error.Reason);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Kafka message handler threw an unhandled " +
                    "exception. Offset will not be committed and " +
                    "the message will be redelivered.");
            }
        }
    }

    private void OnError(IConsumer<TKey, TValue> consumer, Error error)
    {
        _logger.LogError(
            "Kafka consumer error. " +
            "Code={Code}, Reason={Reason}",
            error.Code,
            error.Reason);
    }

    private void OnPartitionsAssigned(IConsumer<TKey, TValue> consumer, List<TopicPartition> partitions)
    {
        _logger.LogInformation("Kafka partitions assigned: {Partitions}", string.Join(", ", partitions));
    }

    private void OnPartitionsRevoked(IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> partitions)
    {
        _logger.LogInformation("Kafka partitions revoked: {Partitions}", string.Join(", ", partitions));

        try
        {
            consumer.Commit();
        }
        catch (KafkaException ex)
        {
            _logger.LogWarning(ex, "Failed to commit offsets during partition revoke.");
        }
    }

    private void OnPartitionsLost(IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> partitions)
    {
        _logger.LogWarning(
            "Kafka partitions lost (session expired before graceful revoke): {Partitions}",
            string.Join(", ", partitions));
    }

    private static AutoOffsetReset ParseAutoOffsetReset(
        string value)
    {
        return value.ToLowerInvariant() switch
        {
            "earliest" =>
                AutoOffsetReset.Earliest,

            "latest" =>
                AutoOffsetReset.Latest,

            "error" =>
                AutoOffsetReset.Error,

            _ => throw new InvalidOperationException(
                $"Invalid AutoOffsetReset value: {value}")
        };
    }

    private static PartitionAssignmentStrategy ParsePartitionAssignmentStrategy(string value)
    {
        return value.ToLowerInvariant() switch
        {
            "range" => PartitionAssignmentStrategy.Range,
            "roundrobin" => PartitionAssignmentStrategy.RoundRobin,
            "cooperativesticky" => PartitionAssignmentStrategy.CooperativeSticky,

            _ => throw new InvalidOperationException(
                $"Invalid Kafka PartitionAssignmentStrategy value: {value}")
        };
    }

    public void Dispose()
    {
        try
        {
            _consumer.Close();
        }
        finally
        {
            _consumer.Dispose();
        }
    }
}
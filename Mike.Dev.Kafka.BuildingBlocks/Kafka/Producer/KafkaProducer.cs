using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;

public sealed class KafkaProducer<TKey, TValue>
    : IKafkaProducer<TKey, TValue>, IDisposable
{
    private readonly IProducer<TKey, TValue> _producer;

    private readonly ILogger<KafkaProducer<TKey, TValue>> _logger;

    public KafkaProducer(
        IOptions<KafkaProducerOptions> options,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        ILogger<KafkaProducer<TKey, TValue>> logger)
    {
        _logger = logger;

        var settings = options.Value;

        var config = new ProducerConfig
        {
            BootstrapServers =
         settings.BootstrapServers,

            Acks =
         ParseAcks(settings.Acks),

            EnableIdempotence =
         settings.EnableIdempotence,

            MessageTimeoutMs =
         settings.MessageTimeoutMs,

            RequestTimeoutMs =
         settings.RequestTimeoutMs,

            MessageSendMaxRetries =
         settings.MessageSendMaxRetries,

            RetryBackoffMs =
         settings.RetryBackoffMs,

            CompressionType =
         ParseCompressionType(
             settings.CompressionType),

            BatchSize =
         settings.BatchSize,

            LingerMs =
         settings.LingerMs,

            QueueBufferingMaxMessages =
         settings.QueueBufferingMaxMessages,

            QueueBufferingMaxKbytes =
         settings.QueueBufferingMaxKbytes
        };

        _producer = new ProducerBuilder<TKey, TValue>(config)
            .SetKeySerializer(keySerializer)
            .SetValueSerializer(valueSerializer)
            .SetErrorHandler(OnError)
            .SetLogHandler(OnLog)
            .Build();
    }

    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(KafkaMessage<TKey, TValue> kafkaMessage, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(kafkaMessage.Topic))
        {
            throw new ArgumentException(
                "Kafka topic is required.");
        }

        var message = new Message<TKey, TValue>
        {
            Key = kafkaMessage.Key,
            Value = kafkaMessage.Value,
            Headers = BuildHeaders(kafkaMessage.Headers)
        };

        try
        {
            var result = await _producer.ProduceAsync(
                kafkaMessage.Topic,
                message,
                cancellationToken);

            _logger.LogDebug(
                "Kafka message delivered. " +
                "Topic={Topic}, " +
                "Partition={Partition}, " +
                "Offset={Offset}",
                result.Topic,
                result.Partition,
                result.Offset);

            return result;
        }
        catch (ProduceException<TKey, TValue> ex)
        {
            if (IsRetriable(ex.Error.Code))
            {
                _logger.LogWarning(
                    ex,
                    "Kafka producer encountered a retriable error. " +
                    "Topic={Topic}, " +
                    "Code={Code}, " +
                    "Reason={Reason}",
                    kafkaMessage.Topic,
                    ex.Error.Code,
                    ex.Error.Reason);
            }
            else
            {
                _logger.LogError(
                    ex,
                    "Kafka producer encountered a non-retriable error. " +
                    "Topic={Topic}, " +
                    "Code={Code}, " +
                    "Reason={Reason}",
                    kafkaMessage.Topic,
                    ex.Error.Code,
                    ex.Error.Reason);
            }

            throw;
        }
    }

    public static Headers? BuildHeaders(IDictionary<string, string>? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return null;
        }
        var kafkaHeaders = new Headers();

        foreach (var header in headers)
        {
            kafkaHeaders.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
        }
        return kafkaHeaders;
    }


    private void OnError(
        IProducer<TKey, TValue> producer,
        Error error)
    {
        _logger.LogError(
            "Kafka producer error. " +
            "Code={Code}, Reason={Reason}",
            error.Code,
            error.Reason);
    }

    private void OnLog(
        IProducer<TKey, TValue> producer,
        LogMessage logMessage)
    {
        _logger.LogDebug(
            "Kafka producer log. " +
            "Level={Level}, Message={Message}",
            logMessage.Level,
            logMessage.Message);
    }

    private static Acks ParseAcks(string value)
    {
        return value.ToLowerInvariant() switch
        {
            "all" => Acks.All,
            "1" => Acks.Leader,
            "0" => Acks.None,

            _ => throw new InvalidOperationException(
                $"Invalid Kafka Acks value: {value}")
        };
    }

    private static CompressionType ParseCompressionType(
        string value)
    {
        return value.ToLowerInvariant() switch
        {
            "none" => CompressionType.None,
            "gzip" => CompressionType.Gzip,
            "snappy" => CompressionType.Snappy,
            "lz4" => CompressionType.Lz4,
            "zstd" => CompressionType.Zstd,

            _ => throw new InvalidOperationException(
                $"Invalid Kafka compression type: {value}")
        };
    }

    private static bool IsRetriable(ErrorCode code)
    {
        return code switch
        {
            ErrorCode.RequestTimedOut
                => true,

            ErrorCode.NetworkException
                => true,

            ErrorCode.BrokerNotAvailable
                => true,

            ErrorCode.NotEnoughReplicas
                => true,

            ErrorCode.NotEnoughReplicasAfterAppend
                => true,

            ErrorCode.LeaderNotAvailable
                => true,

            ErrorCode.ReplicaNotAvailable
                => true,

            _ => false
        };
    }

    public void Dispose()
    {
        try
        {
            _producer.Flush(
                TimeSpan.FromSeconds(10));
        }
        finally
        {
            _producer.Dispose();
        }
    }
}
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Transaction;

public sealed class KafkaTransactionalProducer<TKey, TValue>
    : IKafkaTransactionalProducer<TKey, TValue>,
      IDisposable
{
    private readonly IProducer<TKey, TValue> _producer;

    private readonly KafkaTransactionalProducerOptions _options;

    private readonly ILogger<
        KafkaTransactionalProducer<TKey, TValue>> _logger;

    private readonly SemaphoreSlim _transactionLock =
        new(1, 1);

    public KafkaTransactionalProducer(
        IOptions<KafkaTransactionalProducerOptions> options,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        ILogger<KafkaTransactionalProducer<TKey, TValue>> logger)
    {
        _options = options.Value;
        _logger = logger;

        var config =
            new ProducerConfig
            {
                BootstrapServers =
                    _options.BootstrapServers,

                TransactionalId =
                    $"{_options.TransactionalIdPrefix}-" +
                    $"{Guid.NewGuid():N}",

                TransactionTimeoutMs =
                    _options.TransactionTimeoutMs,

                MessageTimeoutMs =
                    _options.MessageTimeoutMs,

                EnableIdempotence =
                    true,

                Acks =
                    Acks.All
            };

        _producer =
            new ProducerBuilder<TKey, TValue>(config)
                .SetKeySerializer(keySerializer)
                .SetValueSerializer(valueSerializer)
                .SetErrorHandler(OnError)
                .SetLogHandler(OnLog)
                .Build();

        _producer.InitTransactions(
            TimeSpan.FromMilliseconds(
                _options.TransactionTimeoutMs));
    }

    public Task ProduceAndCommitAsync(
        KafkaMessage<TKey, TValue> message,
        IEnumerable<TopicPartitionOffset> offsetsToCommit,
        IConsumerGroupMetadata consumerGroupMetadata,
        CancellationToken cancellationToken = default)
    {
        return ExecuteInTransactionAsync(
            produceMessages: () =>
                ProduceOne(message),

            offsetsToCommit:
                offsetsToCommit,

            consumerGroupMetadata:
                consumerGroupMetadata,

            logContext:
                message.Topic,

            cancellationToken:
                cancellationToken);
    }

    public Task ProduceManyAsync(
        IEnumerable<KafkaMessage<TKey, TValue>> messages,
        CancellationToken cancellationToken = default)
    {
        var messageList =
            messages.ToList();

        return ExecuteInTransactionAsync(
            produceMessages: () =>
            {
                foreach (var message in messageList)
                {
                    ProduceOne(message);
                }
            },

            offsetsToCommit:
                null,

            consumerGroupMetadata:
                null,

            logContext:
                string.Join(
                    ", ",
                    messageList
                        .Select(x => x.Topic)
                        .Distinct()),

            cancellationToken:
                cancellationToken);
    }

    private void ProduceOne(
        KafkaMessage<TKey, TValue> message)
    {
        _producer.Produce(
            message.Topic,
            new Message<TKey, TValue>
            {
                Key = message.Key,

                Value = message.Value,

                Headers =
                    BuildHeaders(message.Headers)
            });
    }

    private async Task ExecuteInTransactionAsync(
        Action produceMessages,
        IEnumerable<TopicPartitionOffset>?
            offsetsToCommit,
        IConsumerGroupMetadata?
            consumerGroupMetadata,
        string logContext,
        CancellationToken cancellationToken)
    {
        await _transactionLock.WaitAsync(
            cancellationToken);

        try
        {
            for (var attempt = 1;
                 attempt <= _options.MaxTransactionRetryAttempts;
                 attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    _producer.BeginTransaction();

                    produceMessages();

                    if (offsetsToCommit is not null &&
                        consumerGroupMetadata is not null)
                    {
                        _producer.SendOffsetsToTransaction(
                            offsetsToCommit,
                            consumerGroupMetadata,
                            TimeSpan.FromMilliseconds(
                                _options.TransactionTimeoutMs));
                    }

                    _producer.CommitTransaction(
                        TimeSpan.FromMilliseconds(
                            _options.TransactionTimeoutMs));

                    _logger.LogInformation(
                        "Kafka transaction committed. " +
                        "Topics={Topics}",
                        logContext);

                    return;
                }
                catch (KafkaException ex)
                    when (!ex.Error.IsFatal &&
                          ex is KafkaRetriableException &&
                          attempt <
                          _options.MaxTransactionRetryAttempts)
                {
                    _logger.LogWarning(
                        ex,
                        "Retriable Kafka transaction failure. " +
                        "Aborting transaction before retry. " +
                        "Topics={Topics}, " +
                        "Attempt={Attempt}/{MaxAttempts}",
                        logContext,
                        attempt,
                        _options.MaxTransactionRetryAttempts);

                    AbortTransaction(logContext);

                    await DelayBeforeRetryAsync(
                        attempt,
                        cancellationToken);
                }
                catch (KafkaException ex)
                {
                    AbortIfRequired(
                        ex,
                        logContext);

                    throw;
                }
            }

            throw new KafkaException(
                new Error(
                    ErrorCode.Local_AllBrokersDown,
                    "Kafka transaction retry attempts exhausted."));
        }
        finally
        {
            _transactionLock.Release();
        }
    }

    private void AbortIfRequired(
        KafkaException exception,
        string logContext)
    {
        if (exception.Error.IsFatal)
        {
            _logger.LogCritical(
                exception,
                "Kafka transactional producer " +
                "entered a fatal state. " +
                "Topics={Topics}, Code={Code}",
                logContext,
                exception.Error.Code);

            return;
        }

        if (exception is KafkaTxnRequiresAbortException)
        {
            AbortTransaction(logContext);
            return;
        }

        _logger.LogError(
            exception,
            "Kafka transaction failed. " +
            "Topics={Topics}",
            logContext);
    }

    private void AbortTransaction(
        string logContext)
    {
        try
        {
            _producer.AbortTransaction(
                TimeSpan.FromMilliseconds(
                    _options.TransactionTimeoutMs));

            _logger.LogDebug(
                "Kafka transaction aborted. " +
                "Topics={Topics}",
                logContext);
        }
        catch (KafkaException ex)
        {
            _logger.LogCritical(
                ex,
                "Failed to abort Kafka transaction. " +
                "Topics={Topics}",
                logContext);

            throw;
        }
    }

    private async Task DelayBeforeRetryAsync(
        int attempt,
        CancellationToken cancellationToken)
    {
        var delayMs =
            Math.Min(
                _options.InitialRetryDelayMs *
                Math.Pow(
                    _options.RetryBackoffMultiplier,
                    attempt - 1),
                _options.MaxRetryDelayMs);

        await Task.Delay(
            TimeSpan.FromMilliseconds(delayMs),
            cancellationToken);
    }

    private void OnError(
        IProducer<TKey, TValue> producer,
        Error error)
    {
        _logger.LogError(
            "Kafka transactional producer error. " +
            "Code={Code}, Reason={Reason}, " +
            "IsFatal={IsFatal}",
            error.Code,
            error.Reason,
            error.IsFatal);
    }

    private void OnLog(
        IProducer<TKey, TValue> producer,
        LogMessage logMessage)
    {
        _logger.LogDebug(
            "Kafka transactional producer log. " +
            "Level={Level}, Message={Message}",
            logMessage.Level,
            logMessage.Message);
    }

    private static Headers? BuildHeaders(
        IDictionary<string, string>? headers)
    {
        if (headers is null ||
            headers.Count == 0)
        {
            return null;
        }

        var kafkaHeaders =
            new Headers();

        foreach (var header in headers)
        {
            kafkaHeaders.Add(
                header.Key,
                Encoding.UTF8.GetBytes(
                    header.Value));
        }

        return kafkaHeaders;
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
            _transactionLock.Dispose();
        }
    }
}
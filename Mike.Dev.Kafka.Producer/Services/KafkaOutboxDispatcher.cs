using Microsoft.Extensions.Options;
using Mike.Dev.Kafka.BuildingBlocks.Kafka;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;
using Mike.Dev.Kafka.Contracts.Events;
using Mike.Dev.Kafka.Producer.Data;
using Mike.Dev.Kafka.Producer.Options;
using Mike.Dev.Kafka.Producer.Outbox;
using System.Text.Json;

namespace Mike.Dev.Kafka.Producer.Services;

public sealed class KafkaOutboxDispatcher
    : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;

    private readonly IKafkaProducer<string, DeviceEvent> _producer;

    private readonly ILogger<KafkaOutboxDispatcher> _logger;

    private readonly KafkaOutboxDispatcherOptions _options;

    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    public KafkaOutboxDispatcher(
        IServiceScopeFactory scopeFactory,
        IKafkaProducer<string, DeviceEvent> producer,
        IOptions<KafkaOutboxDispatcherOptions> options,
        ILogger<KafkaOutboxDispatcher> logger)
    {
        _scopeFactory = scopeFactory;
        _producer = producer;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Kafka outbox dispatcher started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RecoverStuckMessagesAsync(
                    stoppingToken);

                var processed =
                    await DispatchBatchAsync(
                        stoppingToken);

                if (processed == 0)
                {
                    await Task.Delay(
                        _options.PollIntervalMs,
                        stoppingToken);
                }
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Kafka outbox dispatcher iteration failed.");

                await Task.Delay(
                    _options.ErrorDelayMs,
                    stoppingToken);
            }
        }

        _logger.LogInformation(
            "Kafka outbox dispatcher stopped.");
    }

    private async Task<int> DispatchBatchAsync(
        CancellationToken cancellationToken)
    {
        using var scope =
            _scopeFactory.CreateScope();

        var repository =
            scope.ServiceProvider
                .GetRequiredService<IKafkaOutboxRepository>();

        var messages =
            await repository.ClaimPendingAsync(
                _options.BatchSize,
                TimeSpan.FromSeconds(
                    _options.LeaseDurationSeconds),
                cancellationToken);

        foreach (var message in messages)
        {
            await PublishMessageAsync(
                repository,
                message,
                cancellationToken);
        }

        return messages.Count;
    }

    private async Task PublishMessageAsync(
        IKafkaOutboxRepository repository,
        KafkaOutboxMessage message,
        CancellationToken cancellationToken)
    {
        try
        {
            var value =
                JsonSerializer.Deserialize<DeviceEvent>(
                    message.Payload,
                    JsonOptions);

            if (value is null)
            {
                throw new InvalidOperationException(
                    $"Unable to deserialize outbox message {message.Id}.");
            }

            var headers =
                DeserializeHeaders(
                    message.Headers);

            var kafkaMessage =
                new KafkaMessage<string, DeviceEvent>
                {
                    Topic = message.Topic,

                    Key = message.Key,

                    Value = value,

                    Headers = headers
                };

            var result =
                await _producer.ProduceAsync(
                    kafkaMessage,
                    cancellationToken);

            await repository.MarkPublishedAsync(
                message.Id,
                cancellationToken);

            _logger.LogInformation(
                "Outbox message published. " +
                "OutboxId={OutboxId}, " +
                "EventId={EventId}, " +
                "Topic={Topic}, " +
                "Partition={Partition}, " +
                "Offset={Offset}",
                message.Id,
                message.EventId,
                result.Topic,
                result.Partition,
                result.Offset);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var retryCount =
                message.RetryCount + 1;

            if (retryCount >=
                _options.MaxRetryCount)
            {
                await repository.MarkDeadLetteredAsync(
                    message.Id,
                    ex.Message,
                    cancellationToken);

                _logger.LogCritical(
                    ex,
                    "Outbox message exhausted retries and was dead-lettered. " +
                    "OutboxId={OutboxId}, " +
                    "EventId={EventId}, " +
                    "RetryCount={RetryCount}",
                    message.Id,
                    message.EventId,
                    retryCount);

                return;
            }

            var delay =
                CalculateRetryDelay(
                    retryCount);

            var nextAttempt =
                DateTime.UtcNow.Add(delay);

            await repository.MarkFailedAsync(
                message.Id,
                ex.Message,
                nextAttempt,
                cancellationToken);

            _logger.LogError(
                ex,
                "Failed to publish outbox message. " +
                "OutboxId={OutboxId}, " +
                "EventId={EventId}, " +
                "RetryCount={RetryCount}, " +
                "NextAttempt={NextAttempt}",
                message.Id,
                message.EventId,
                retryCount,
                nextAttempt);
        }
    }

    private async Task RecoverStuckMessagesAsync(
        CancellationToken cancellationToken)
    {
        using var scope =
            _scopeFactory.CreateScope();

        var repository =
            scope.ServiceProvider
                .GetRequiredService<IKafkaOutboxRepository>();

        await repository.RecoverStuckMessagesAsync(
            TimeSpan.FromSeconds(
                _options.LeaseDurationSeconds),
            cancellationToken);
    }

    private TimeSpan CalculateRetryDelay(
        int retryCount)
    {
        var delay =
            _options.InitialRetryDelayMs *
            Math.Pow(
                _options.BackoffMultiplier,
                retryCount - 1);

        delay =
            Math.Min(
                delay,
                _options.MaxRetryDelayMs);

        return TimeSpan.FromMilliseconds(
            delay);
    }

    private static Dictionary<string, string>?
        DeserializeHeaders(
            string? headers)
    {
        if (string.IsNullOrWhiteSpace(headers))
        {
            return null;
        }

        return JsonSerializer.Deserialize<
            Dictionary<string, string>>(headers);
    }
}
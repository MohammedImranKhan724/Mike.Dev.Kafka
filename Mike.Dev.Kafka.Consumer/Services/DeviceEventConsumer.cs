using Mike.Dev.Kafka.BuildingBlocks;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.DeadLetter;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Exceptions;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Retry;
using Mike.Dev.Kafka.Consumer.Handlers;
using Mike.Dev.Kafka.Contracts.Events;

namespace Mike.Dev.Kafka.Consumer.Services;

public sealed class DeviceEventConsumer
    : BackgroundService
{
    private readonly
        IKafkaConsumer<string, DeviceEvent>
        _consumer;

    private readonly
        IKafkaMessageHandler<string, DeviceEvent>
        _handler;

    private readonly
        KafkaRetryExecutor
        _retryExecutor;

    private readonly
        KafkaDeadLetterProducer<string, DeviceEvent>
        _dltProducer;

    private readonly
        DeviceEventTransactionalProcessor
        _transactionalProcessor;

    private readonly
        ILogger<DeviceEventConsumer>
        _logger;

    public DeviceEventConsumer(
        IKafkaConsumer<string, DeviceEvent> consumer,
        IKafkaMessageHandler<string, DeviceEvent> handler,
        KafkaRetryExecutor retryExecutor,
        KafkaDeadLetterProducer<string, DeviceEvent> dltProducer,
        DeviceEventTransactionalProcessor transactionalProcessor,
        ILogger<DeviceEventConsumer> logger)
    {
        _consumer = consumer;
        _handler = handler;
        _retryExecutor = retryExecutor;
        _dltProducer = dltProducer;
        _transactionalProcessor =
            transactionalProcessor;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Device event consumer started.");

        await _consumer.ConsumeAsync(
            ProcessMessageAsync,
            stoppingToken);

        _logger.LogInformation(
            "Device event consumer stopped.");
    }

    private async Task<KafkaProcessingStatus>
        ProcessMessageAsync(
            KafkaConsumedMessage<string, DeviceEvent> message,
            CancellationToken cancellationToken)
    {
        try
        {
            await _retryExecutor.ExecuteAsync(
                async ct =>
                {
                    await _handler.HandleAsync(
                        message,
                        ct);

                    var groupMetadata =
                        _consumer.GetGroupMetadata();

                    await _transactionalProcessor.ProcessAsync(
                        message,
                        groupMetadata,
                        ct);
                },
                cancellationToken);

            _logger.LogInformation(
                "Kafka message processed. " +
                "Topic={Topic}, " +
                "Partition={Partition}, " +
                "Offset={Offset}",
                message.Topic,
                message.Partition,
                message.Offset);

            return KafkaProcessingStatus.Success;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Kafka message processing cancelled.");

            throw;
        }
        catch (KafkaRetryExhaustedException ex)
        {
            return await DeadLetterAsync(
                message,
                ex.InnerException ?? ex,
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Kafka message processing failed with a " +
                "non-retryable error. " +
                "Topic={Topic}, " +
                "Partition={Partition}, " +
                "Offset={Offset}",
                message.Topic,
                message.Partition,
                message.Offset);

            return await DeadLetterAsync(
                message,
                ex,
                cancellationToken);
        }
    }

    private async Task<KafkaProcessingStatus> DeadLetterAsync(
        KafkaConsumedMessage<string, DeviceEvent> message,
        Exception exception,
        CancellationToken cancellationToken)
    {
        await PublishToDeadLetterAsync(
            message,
            exception,
            cancellationToken);

        _consumer.Commit(message);

        return KafkaProcessingStatus.DeadLetter;
    }

    private async Task PublishToDeadLetterAsync(
        KafkaConsumedMessage<string, DeviceEvent> message,
        Exception exception,
        CancellationToken cancellationToken)
    {
        _logger.LogError(
            exception,
            "Publishing Kafka message to DLT. " +
            "Topic={Topic}, " +
            "Partition={Partition}, " +
            "Offset={Offset}",
            message.Topic,
            message.Partition,
            message.Offset);

        await _dltProducer.PublishAsync(
            message.RawResult,
            exception,
            cancellationToken);
    }
}
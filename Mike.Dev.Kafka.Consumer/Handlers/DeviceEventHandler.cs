using Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;
using Mike.Dev.Kafka.Consumer.Idempotency;
using Mike.Dev.Kafka.Contracts.Events;

namespace Mike.Dev.Kafka.Consumer.Handlers;

public sealed class DeviceEventHandler
    : IKafkaMessageHandler<string, DeviceEvent>
{
    private readonly IProcessedEventStore _processedEventStore;
    private readonly ILogger<DeviceEventHandler> _logger;

    public DeviceEventHandler(
        IProcessedEventStore processedEventStore,
        ILogger<DeviceEventHandler> logger)
    {
        _processedEventStore = processedEventStore;
        _logger = logger;
    }

    public async Task HandleAsync(
        KafkaConsumedMessage<string, DeviceEvent> message,
        CancellationToken cancellationToken)
    {
        var deviceEvent = message.Value;

        if (deviceEvent is null)
        {
            throw new InvalidOperationException(
                "DeviceEvent is null.");
        }

        var alreadyProcessed =
            await _processedEventStore.HasProcessedAsync(
                deviceEvent.EventId,
                cancellationToken);

        if (alreadyProcessed)
        {
            _logger.LogInformation(
                "Duplicate Kafka message detected. " +
                "EventId={EventId}",
                deviceEvent.EventId);

            return;
        }

        _logger.LogInformation(
            "Processing DeviceEvent. " +
            "EventId={EventId}, " +
            "CorrelationId={CorrelationId}, " +
            "DeviceId={DeviceId}, " +
            "EventType={EventType}",
            deviceEvent.EventId,
            deviceEvent.CorrelationId,
            deviceEvent.DeviceId,
            deviceEvent.EventType);

        // Simulate processing failure
        if (deviceEvent.DeviceId == 5)
        {
            throw new InvalidOperationException(
                "Simulated processing failure.");
        }

        await Task.Delay(
            100,
            cancellationToken);

        await _processedEventStore.MarkProcessedAsync(
            deviceEvent.EventId,
            cancellationToken);

        _logger.LogInformation(
            "DeviceEvent processed successfully. " +
            "EventId={EventId}",
            deviceEvent.EventId);
    }
}
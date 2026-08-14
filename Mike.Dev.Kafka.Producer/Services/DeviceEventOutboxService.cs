using Microsoft.Extensions.Options;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;
using Mike.Dev.Kafka.Contracts.Events;
using Mike.Dev.Kafka.Producer.Data;
using Mike.Dev.Kafka.Producer.Outbox;

namespace Mike.Dev.Kafka.Producer.Services;

public sealed class DeviceEventOutboxService
{
    private readonly ApplicationDbContext _dbContext;
    private readonly IKafkaOutboxMessageFactory _factory;
    private readonly IKafkaOutboxRepository _repository;
    private readonly KafkaProducerOptions _options;
    private readonly ILogger<DeviceEventOutboxService> _logger;

    public DeviceEventOutboxService(
        ApplicationDbContext dbContext,
        IKafkaOutboxMessageFactory factory,
        IKafkaOutboxRepository repository,
        IOptions<KafkaProducerOptions> options,
        ILogger<DeviceEventOutboxService> logger)
    {
        _dbContext = dbContext;
        _factory = factory;
        _repository = repository;
        _options = options.Value;
        _logger = logger;
    }

    public async Task CreateAsync(
        DeviceEvent deviceEvent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(deviceEvent);

        var outboxMessage =
            _factory.CreateDeviceEventMessage(
                deviceEvent,
                _options.Topics.DeviceEvents);

        await using var transaction =
            await _dbContext.Database.BeginTransactionAsync(
                cancellationToken);

        try
        {
            // Business-specific database writes belong here, inside the
            // same transaction as the outbox insert below, so both commit
            // or roll back together.

            await _repository.AddAsync(
                outboxMessage,
                cancellationToken);

            await _dbContext.SaveChangesAsync(
                cancellationToken);

            await transaction.CommitAsync(
                cancellationToken);

            _logger.LogInformation(
                "Business operation and Kafka outbox " +
                "message committed. " +
                "EventId={EventId}, DeviceId={DeviceId}",
                deviceEvent.EventId,
                deviceEvent.DeviceId);
        }
        catch (Exception ex)
        {
            try
            {
                await transaction.RollbackAsync(
                    cancellationToken);
            }
            catch (Exception rollbackException)
            {
                _logger.LogError(
                    rollbackException,
                    "Failed to rollback database transaction. " +
                    "EventId={EventId}",
                    deviceEvent.EventId);
            }

            _logger.LogError(
                ex,
                "Database transaction failed. " +
                "Business operation and outbox message " +
                "were rolled back. " +
                "EventId={EventId}, DeviceId={DeviceId}",
                deviceEvent.EventId,
                deviceEvent.DeviceId);

            throw;
        }
    }
}
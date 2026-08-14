using Microsoft.EntityFrameworkCore;
using Mike.Dev.Kafka.Producer.Data;

namespace Mike.Dev.Kafka.Producer.Outbox;

public sealed class KafkaOutboxRepository
    : IKafkaOutboxRepository
{
    private readonly ApplicationDbContext _dbContext;

    private readonly ILogger<KafkaOutboxRepository> _logger;

    public KafkaOutboxRepository(
        ApplicationDbContext dbContext,
        ILogger<KafkaOutboxRepository> logger)
    {
        _dbContext = dbContext;
        _logger = logger;
    }

    public async Task AddAsync(
        KafkaOutboxMessage message,
        CancellationToken cancellationToken = default)
    {
        await _dbContext.KafkaOutboxMessages.AddAsync(
            message,
            cancellationToken);
    }

    public async Task<IReadOnlyList<KafkaOutboxMessage>> ClaimPendingAsync(
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;

        var leaseExpiry =
            now.Subtract(leaseDuration);

        await using var transaction =
            await _dbContext.Database.BeginTransactionAsync(
                cancellationToken);

        try
        {
            var messages =
                await _dbContext.KafkaOutboxMessages
                    .FromSqlInterpolated($"""
                        SELECT *
                        FROM kafka_outbox_messages
                        WHERE
                            (
                                status = {(int)KafkaOutboxStatus.Pending}
                            )
                            OR
                            (
                                status = {(int)KafkaOutboxStatus.Failed}
                                AND
                                (
                                    next_attempt_at_utc IS NULL
                                    OR next_attempt_at_utc <= {now}
                                )
                            )
                            OR
                            (
                                status = {(int)KafkaOutboxStatus.Publishing}
                                AND
                                (
                                    processing_started_at_utc IS NULL
                                    OR processing_started_at_utc <= {leaseExpiry}
                                )
                            )
                        ORDER BY created_at_utc
                        LIMIT {batchSize}
                        FOR UPDATE SKIP LOCKED
                        """)
                    .ToListAsync(cancellationToken);

            if (messages.Count == 0)
            {
                await transaction.CommitAsync(
                    cancellationToken);

                return messages;
            }

            var messageIds =
                messages
                    .Select(x => x.Id)
                    .ToList();

            await _dbContext.KafkaOutboxMessages
                .Where(x => messageIds.Contains(x.Id))
                .ExecuteUpdateAsync(
                    setters => setters
                        .SetProperty(
                            x => x.Status,
                            KafkaOutboxStatus.Publishing)

                        .SetProperty(
                            x => x.ProcessingStartedAtUtc,
                            now)

                        .SetProperty(
                            x => x.UpdatedAtUtc,
                            now),
                    cancellationToken);

            await transaction.CommitAsync(
                cancellationToken);

            return messages;
        }
        catch
        {
            await transaction.RollbackAsync(
                cancellationToken);

            throw;
        }
    }

    public async Task RecoverStuckMessagesAsync(
    TimeSpan leaseDuration,
    CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;

        var leaseExpiry = now.Subtract(leaseDuration);

        var affectedRows =
            await _dbContext.KafkaOutboxMessages
                .Where(x =>
                    x.Status == KafkaOutboxStatus.Publishing &&
                    x.ProcessingStartedAtUtc != null &&
                    x.ProcessingStartedAtUtc <= leaseExpiry)
                .ExecuteUpdateAsync(
                    setters => setters
                        .SetProperty(
                            x => x.Status,
                            KafkaOutboxStatus.Failed)

                        .SetProperty(
                            x => x.NextAttemptAtUtc,
                            now)

                        .SetProperty(
                            x => x.LastError,
                            "Outbox message processing lease expired.")

                        .SetProperty(
                            x => x.ProcessingStartedAtUtc,
                            (DateTime?)null)

                        .SetProperty(
                            x => x.UpdatedAtUtc,
                            now),
                    cancellationToken);

        if (affectedRows > 0)
        {
            _logger.LogWarning(
                "Recovered {Count} stuck outbox message(s).",
                affectedRows);
        }
    }

    public async Task MarkPublishedAsync(
        long id,
        CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;

        await _dbContext.KafkaOutboxMessages
            .Where(x =>
                x.Id == id &&
                x.Status == KafkaOutboxStatus.Publishing)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(
                        x => x.Status,
                        KafkaOutboxStatus.Published)

                    .SetProperty(
                        x => x.PublishedAtUtc,
                        now)

                    .SetProperty(
                        x => x.ProcessingStartedAtUtc,
                        (DateTime?)null)

                    .SetProperty(
                        x => x.NextAttemptAtUtc,
                        (DateTime?)null)

                    .SetProperty(
                        x => x.LastError,
                        (string?)null)

                    .SetProperty(
                        x => x.UpdatedAtUtc,
                        now),
                cancellationToken);
    }

    public async Task MarkFailedAsync(
        long id,
        string error,
        DateTime nextAttemptAtUtc,
        CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;

        await _dbContext.KafkaOutboxMessages
            .Where(x =>
                x.Id == id &&
                x.Status == KafkaOutboxStatus.Publishing)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(
                        x => x.Status,
                        KafkaOutboxStatus.Failed)

                    .SetProperty(
                        x => x.RetryCount,
                        x => x.RetryCount + 1)

                    .SetProperty(
                        x => x.LastError,
                        error)

                    .SetProperty(
                        x => x.NextAttemptAtUtc,
                        nextAttemptAtUtc)

                    .SetProperty(
                        x => x.ProcessingStartedAtUtc,
                        (DateTime?)null)

                    .SetProperty(
                        x => x.UpdatedAtUtc,
                        now),
                cancellationToken);
    }

    public async Task MarkDeadLetteredAsync(
        long id,
        string error,
        CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;

        await _dbContext.KafkaOutboxMessages
            .Where(x =>
                x.Id == id &&
                x.Status == KafkaOutboxStatus.Publishing)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(
                        x => x.Status,
                        KafkaOutboxStatus.DeadLettered)

                    .SetProperty(
                        x => x.LastError,
                        error)

                    .SetProperty(
                        x => x.ProcessingStartedAtUtc,
                        (DateTime?)null)

                    .SetProperty(
                        x => x.NextAttemptAtUtc,
                        (DateTime?)null)

                    .SetProperty(
                        x => x.UpdatedAtUtc,
                        now),
                cancellationToken);
    }
}
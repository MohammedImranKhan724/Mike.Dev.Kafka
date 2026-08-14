using Mike.Dev.Kafka.Producer.Data;

namespace Mike.Dev.Kafka.Producer.Outbox;

public interface IKafkaOutboxRepository
{
    Task AddAsync(
        KafkaOutboxMessage message,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<KafkaOutboxMessage>> ClaimPendingAsync(
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    Task RecoverStuckMessagesAsync(
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    Task MarkPublishedAsync(
        long id,
        CancellationToken cancellationToken = default);

    Task MarkFailedAsync(
        long id,
        string error,
        DateTime nextAttemptAtUtc,
        CancellationToken cancellationToken = default);

    Task MarkDeadLetteredAsync(
        long id,
        string error,
        CancellationToken cancellationToken = default);
}
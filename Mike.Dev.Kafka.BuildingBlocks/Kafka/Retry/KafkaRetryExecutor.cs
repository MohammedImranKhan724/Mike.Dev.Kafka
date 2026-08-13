using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Exceptions;

namespace Mike.Dev.Kafka.BuildingBlocks.Kafka.Retry;

public sealed class KafkaRetryExecutor
{
    private readonly KafkaRetryOptions _options;
    private readonly KafkaRetryPolicy _retryPolicy;
    private readonly ILogger<KafkaRetryExecutor> _logger;

    public KafkaRetryExecutor(
        IOptions<KafkaRetryOptions> options,
        KafkaRetryPolicy retryPolicy,
        ILogger<KafkaRetryExecutor> logger)
    {
        _options = options.Value;
        _retryPolicy = retryPolicy;
        _logger = logger;
    }

    public async Task ExecuteAsync(
        Func<CancellationToken, Task> action,
        CancellationToken cancellationToken)
    {
        var maxAttempts =
            _options.Enabled
                ? _options.MaxAttempts
                : 1;

        Exception? lastException = null;

        for (var attempt = 1;
             attempt <= maxAttempts;
             attempt++)
        {
            try
            {
                await action(cancellationToken);

                return;
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                lastException = ex;

                // Important:
                // Do not retry exceptions that are not transient.
                if (!_retryPolicy.ShouldRetry(ex))
                {
                    _logger.LogError(
                        ex,
                        "Kafka message processing failed with " +
                        "non-retryable exception. " +
                        "Attempt={Attempt}",
                        attempt);

                    throw;
                }

                _logger.LogWarning(
                    ex,
                    "Kafka transient error. " +
                    "Retrying message. " +
                    "Attempt={Attempt}/{MaxAttempts}",
                    attempt,
                    maxAttempts);

                if (attempt == maxAttempts)
                {
                    break;
                }

                var delay =
                    Math.Min(
                        _options.InitialDelayMs *
                        Math.Pow(
                            _options.BackoffMultiplier,
                            attempt - 1),
                        _options.MaxDelayMs);

                _logger.LogInformation(
                    "Waiting before Kafka retry. " +
                    "Attempt={Attempt}, DelayMs={DelayMs}",
                    attempt,
                    delay);

                await Task.Delay(
                    TimeSpan.FromMilliseconds(delay),
                    cancellationToken);
            }
        }

        throw new KafkaRetryExhaustedException(
            $"Kafka processing failed after " +
            $"{maxAttempts} attempt(s).",
            lastException);
    }
}
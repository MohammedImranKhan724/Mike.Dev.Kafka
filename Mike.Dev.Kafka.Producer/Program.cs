using Microsoft.EntityFrameworkCore;
using Mike.Dev.Kafka.BuildingBlocks.Extensions;
using Mike.Dev.Kafka.Contracts.Events;
using Mike.Dev.Kafka.Producer.Data;
using Mike.Dev.Kafka.Producer.Options;
using Mike.Dev.Kafka.Producer.Outbox;
using Mike.Dev.Kafka.Producer.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<ApplicationDbContext>(options =>
    options
        .UseNpgsql(
            builder.Configuration.GetConnectionString("OutboxDb"))
        .UseSnakeCaseNamingConvention());

builder.Services.AddKafkaProducer(builder.Configuration);
builder.Services.AddKafkaTransactionalProducer(builder.Configuration);
builder.Services.AddKafkaSchemaRegistry(builder.Configuration);

builder.Services
    .AddOptions<KafkaOutboxDispatcherOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOutboxDispatcherOptions.Section))
    .Validate(
        x => x.BatchSize > 0,
        "BatchSize must be greater than zero.")
    .Validate(
        x => x.PollIntervalMs > 0,
        "PollIntervalMs must be greater than zero.")
    .Validate(
        x => x.ErrorDelayMs > 0,
        "ErrorDelayMs must be greater than zero.")
    .Validate(
        x => x.LeaseDurationSeconds > 0,
        "LeaseDurationSeconds must be greater than zero.")
    .Validate(
        x => x.InitialRetryDelayMs > 0,
        "InitialRetryDelayMs must be greater than zero.")
    .Validate(
        x => x.BackoffMultiplier > 1,
        "BackoffMultiplier must be greater than one.")
    .Validate(
        x => x.MaxRetryDelayMs >= x.InitialRetryDelayMs,
        "MaxRetryDelayMs must be greater than or equal to InitialRetryDelayMs.")
    .Validate(
        x => x.MaxRetryCount > 0,
        "MaxRetryCount must be greater than zero.")
    .ValidateOnStart();

builder.Services.AddSingleton<IKafkaOutboxMessageFactory, KafkaOutboxMessageFactory>();
builder.Services.AddScoped<IKafkaOutboxRepository, KafkaOutboxRepository>();

builder.Services.AddScoped<DeviceEventOutboxService>();

builder.Services.AddHostedService<KafkaOutboxDispatcher>();

var host = builder.Build();

await using (var migrationScope = host.Services.CreateAsyncScope())
{
    var dbContext = migrationScope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
    await dbContext.Database.MigrateAsync();
}

await using (var scope = host.Services.CreateAsyncScope())
{
    var outboxService = scope.ServiceProvider.GetRequiredService<DeviceEventOutboxService>();

    for (var i = 0; i < 10; i++)
    {
        var deviceEvent = new DeviceEvent
        {
            EventId = Guid.NewGuid().ToString("N"),

            CorrelationId = Guid.NewGuid().ToString("N"),

            DeviceId = i,

            EventType = "Fault",

            Message = $"Device {i} generated a fault",

            TimestampUtc = DateTime.UtcNow
        };

        await outboxService.CreateAsync(deviceEvent);
    }
}

await host.RunAsync();

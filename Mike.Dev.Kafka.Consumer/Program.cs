using Mike.Dev.Kafka.BuildingBlocks.Extensions;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.DeadLetter;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Retry;
using Mike.Dev.Kafka.Consumer.Handlers;
using Mike.Dev.Kafka.Consumer.Idempotency;
using Mike.Dev.Kafka.Consumer.Services;
using Mike.Dev.Kafka.Contracts.Events;

var builder = Host.CreateApplicationBuilder(args);

// Kafka
builder.Services.AddKafkaConsumer(
    builder.Configuration);

builder.Services.AddKafkaProducer(
    builder.Configuration);


// Idempotency
builder.Services.AddSingleton<
    IProcessedEventStore,
    InMemoryProcessedEventStore>();


// Retry
builder.Services
    .AddOptions<KafkaRetryOptions>()
    .Bind(
        builder.Configuration.GetSection(
            KafkaRetryOptions.Section))
    .ValidateDataAnnotations()
    .Validate(
        options =>
            options.MaxDelayMs >= options.InitialDelayMs,
        "MaxDelayMs must be greater than or equal to InitialDelayMs.")
    .ValidateOnStart();

builder.Services.AddSingleton<KafkaRetryPolicy>();
builder.Services.AddSingleton<KafkaRetryExecutor>();


// DLT
builder.Services
    .AddOptions<KafkaDeadLetterOptions>()
    .Bind(
        builder.Configuration.GetSection(
            "Kafka:DeadLetter"))
    .Validate(
        options =>
            !string.IsNullOrWhiteSpace(
                options.TopicSuffix),
        "DLT TopicSuffix is required.")
    .ValidateOnStart();

builder.Services.AddSingleton(
    typeof(KafkaDeadLetterProducer<,>));


// Handler
builder.Services.AddSingleton<
    IKafkaMessageHandler<string, DeviceEvent>,
    DeviceEventHandler>();


// Background consumer
builder.Services.AddHostedService<DeviceEventConsumer>();


var host = builder.Build();

await host.RunAsync();
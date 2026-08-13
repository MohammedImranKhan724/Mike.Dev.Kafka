using Mike.Dev.Kafka.BuildingBlocks.Extensions;
using Mike.Dev.Kafka.Contracts.Events;
using Mike.Dev.Kafka.Producer.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaProducer(builder.Configuration);

builder.Services.AddSingleton<DeviceEventProducer>();

var host = builder.Build();

var producer = host.Services.GetRequiredService<DeviceEventProducer>();

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

    await producer.ProduceAsync(deviceEvent);
}

await host.RunAsync();
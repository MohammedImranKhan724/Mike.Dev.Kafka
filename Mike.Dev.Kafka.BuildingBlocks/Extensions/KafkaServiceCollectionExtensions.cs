using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.SchemaRegistry;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Serialization;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Transaction;
using Mike.Dev.Kafka.Contracts.Events;

namespace Mike.Dev.Kafka.BuildingBlocks.Extensions;

public static class KafkaServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaProducer(
     this IServiceCollection services,
     IConfiguration configuration)
    {
        services
            .AddOptions<KafkaProducerOptions>()
            .Bind(
                configuration.GetSection(
                    KafkaProducerOptions.Section))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddSingleton(
            typeof(IAsyncSerializer<>),
            typeof(KafkaAsyncSerializerAdapter<>));

        services.AddSingleton<ISerializer<string>>(
            Serializers.Utf8);

        services.AddSingleton(
            typeof(IKafkaProducer<,>),
            typeof(KafkaProducer<,>));

        return services;
    }

    public static IServiceCollection AddKafkaConsumer(
     this IServiceCollection services,
     IConfiguration configuration)
    {
        services
            .AddOptions<KafkaConsumerOptions>()
            .Bind(
                configuration.GetSection(
                    KafkaConsumerOptions.Section))
            .ValidateDataAnnotations()
            .ValidateOnStart();
        services.AddSingleton(
            typeof(IDeserializer<>),
            typeof(KafkaJsonDeserializer<>));

        services.AddSingleton<IDeserializer<string>>(
            Deserializers.Utf8);

        services.AddSingleton(
            typeof(IKafkaConsumer<,>),
            typeof(KafkaConsumer<,>));

        return services;
    }

    public static IServiceCollection AddKafkaTransactionalProducer(
         this IServiceCollection services,
         IConfiguration configuration)
    {
        services
      .AddOptions<KafkaTransactionalProducerOptions>()
      .Bind(
          configuration.GetSection(
              KafkaTransactionalProducerOptions.Section))
      .ValidateDataAnnotations()
      .ValidateOnStart();

        services.AddSingleton<
     ISerializer<string>,
     KafkaStringSerializer>();

        services.AddSingleton(
            typeof(
                IKafkaTransactionalProducer<,>),
            typeof(
                KafkaTransactionalProducer<,>));

        return services;
    }

    public static IServiceCollection AddKafkaSchemaRegistry(
     this IServiceCollection services,
     IConfiguration configuration)
    {
        services
            .AddOptions<KafkaSchemaRegistryOptions>()
            .Bind(configuration.GetSection(
                KafkaSchemaRegistryOptions.Section))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddSingleton<ISchemaRegistryClient>(sp =>
        {
            var options =
                sp.GetRequiredService<
                    IOptions<KafkaSchemaRegistryOptions>>()
                .Value;

            var config = new SchemaRegistryConfig
            {
                Url = options.Url
            };

            return new CachedSchemaRegistryClient(config);
        });

        services.AddSingleton<IKafkaSchemaSerializerFactory, KafkaSchemaSerializerFactory>();

        services.AddSingleton<IKafkaSchemaDeserializerFactory, KafkaSchemaDeserializerFactory>();

        services.AddSingleton<IAsyncSerializer<DeviceEvent>>(sp =>
            sp.GetRequiredService<IKafkaSchemaSerializerFactory>()
                .CreateSerializer<DeviceEvent>());

        services.AddSingleton<IDeserializer<DeviceEvent>>(sp =>
            sp.GetRequiredService<IKafkaSchemaDeserializerFactory>()
                .CreateDeserializer<DeviceEvent>()
                .AsSyncOverAsync());

        return services;
    }
}
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Consumer;
using Mike.Dev.Kafka.BuildingBlocks.Kafka.Producer;
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

        // Generic JSON serializer for all types
        services.AddSingleton(
            typeof(ISerializer<>),
            typeof(KafkaJsonSerializer<>));

        // String should use UTF-8
        services.AddSingleton<ISerializer<string>>(
            Serializers.Utf8);

        // Kafka producer
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

        // Generic JSON deserializer
        services.AddSingleton(
            typeof(IDeserializer<>),
            typeof(KafkaJsonDeserializer<>));

        // String should use UTF-8
        services.AddSingleton<IDeserializer<string>>(
            Deserializers.Utf8);

        // Kafka consumer
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

        services.AddSingleton<
            ISerializer<DeviceEvent>,
            KafkaJsonSerializer<DeviceEvent>>();

        services.AddSingleton(
            typeof(
                IKafkaTransactionalProducer<,>),
            typeof(
                KafkaTransactionalProducer<,>));

        return services;
    }
}
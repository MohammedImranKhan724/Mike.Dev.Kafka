using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Mike.Dev.Kafka.Producer.Data.Configuration;

public sealed class KafkaOutboxMessageConfiguration
    : IEntityTypeConfiguration<KafkaOutboxMessage>
{
    public void Configure(
        EntityTypeBuilder<KafkaOutboxMessage> builder)
    {
        builder.ToTable(
                 "kafka_outbox_messages");

        builder.HasKey(x => x.Id);

        builder.Property(x => x.Id)
            .ValueGeneratedOnAdd();

        builder.Property(x => x.EventId)
            .IsRequired();

        builder.Property(x => x.Topic)
            .IsRequired();

        builder.Property(x => x.Key)
            .IsRequired();

        builder.Property(x => x.Payload)
            .IsRequired();

        builder.Property(x => x.Status)
            .IsRequired();

        builder.Property(x => x.RetryCount)
            .IsRequired();

        builder.Property(x => x.CreatedAtUtc)
            .IsRequired();

        builder.Property(x => x.UpdatedAtUtc)
            .IsRequired();

        builder.HasIndex(x => new
        {
            x.Status,
            x.NextAttemptAtUtc,
            x.CreatedAtUtc
        });

        builder.HasIndex(x =>
            x.EventId)
            .IsUnique();
    }
}

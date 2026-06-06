using Confluent.Kafka;
using JasperFx.Resources;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Wolverine.ComplianceTests;
using Wolverine.ComplianceTests.Compliance;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Wolverine.Kafka.Tests;

public class BufferedComplianceFixture : TransportComplianceFixture
{
    public BufferedComplianceFixture() : base(new Uri("kafka://topic/receiver"), 120)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        var receiverTopic = "buffered.receiver";
        var senderTopic = "buffered.sender";

        OutboundAddress = new Uri("kafka://topic/" + receiverTopic);


        await ReceiverIs(opts =>
        {
            opts.UseKafka(KafkaContainerFixture.ConnectionString);

            opts.ListenToKafkaTopic(receiverTopic).Named("receiver").BufferedInMemory();

            opts.Services.AddResourceSetupOnStartup();
        });
        
        await SenderIs(opts =>
        {
            opts.UseKafka(KafkaContainerFixture.ConnectionString).ConfigureConsumers(x => x.EnableAutoCommit = false);

            opts.ListenToKafkaTopic(senderTopic);

            opts.PublishAllMessages().ToKafkaTopic(receiverTopic).BufferedInMemory();

            opts.Services.AddResourceSetupOnStartup();
        });
    }
}

public class BufferedSendingAndReceivingCompliance(BufferedComplianceFixture fixture)
    : TransportCompliance<BufferedComplianceFixture>(fixture);

public class InlineComplianceFixture : TransportComplianceFixture
{
    public static int Number = 0;

    public InlineComplianceFixture() : base(new Uri("kafka://topic/receiver"), 120)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        var receiverTopic = "receiver.inline";
        var senderTopic = "sender.inline";

        OutboundAddress = new Uri("kafka://topic/" + receiverTopic);

        await SenderIs(opts =>
        {
            opts.UseKafka(KafkaContainerFixture.ConnectionString).AutoProvision();

            opts.ListenToKafkaTopic(senderTopic).UseForReplies().ConfigureConsumer(consumer =>
            {
                consumer.GroupId = "test";
                consumer.AutoOffsetReset = AutoOffsetReset.Earliest;
            });

            opts.PublishAllMessages().ToKafkaTopic(receiverTopic).SendInline();

            opts.Services.AddResourceSetupOnStartup();
        });

        await ReceiverIs(opts =>
        {
            opts.UseKafka(KafkaContainerFixture.ConnectionString).AutoProvision();

            opts.ListenToKafkaTopic(receiverTopic).Named("receiver").ProcessInline();

            opts.Services.AddResourceSetupOnStartup();
        });
    }
}

public class InlineSendingAndReceivingCompliance(InlineComplianceFixture fixture)
    : TransportCompliance<InlineComplianceFixture>(fixture);
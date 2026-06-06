using Wolverine.ComplianceTests.Compliance;
using Xunit;

namespace Wolverine.AzureServiceBus.Tests;

public class TopicsComplianceFixture : TransportComplianceFixture
{
    public TopicsComplianceFixture() : base(new Uri("asb://topic/topic1"), 120)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await SenderIs(opts =>
        {
            opts.UseAzureServiceBusTesting()
                .AutoProvision();
        });

        await ReceiverIs(opts =>
        {
            opts.UseAzureServiceBusTesting()
                .AutoProvision();

            opts.ListenToAzureServiceBusSubscription("subscription1").FromTopic("topic1");
        });
    }

    protected override Task AfterDisposeAsync()
    {
        return AzureServiceBusTesting.DeleteAllEmulatorObjectsAsync();
    }
}

public class TopicAndSubscriptionSendingAndReceivingCompliance(TopicsComplianceFixture fixture)
    : TransportCompliance<TopicsComplianceFixture>(fixture);
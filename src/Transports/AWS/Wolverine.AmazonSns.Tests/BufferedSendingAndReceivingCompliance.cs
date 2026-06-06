using Wolverine.AmazonSns.Internal;
using Wolverine.AmazonSqs;
using Wolverine.ComplianceTests.Compliance;

namespace Wolverine.AmazonSns.Tests;

public class BufferedComplianceFixture : TransportComplianceFixture
{
    public BufferedComplianceFixture() : base(new Uri($"{AmazonSnsTransport.SnsProtocol}://receiver"), 120)
    {
        IsSenderOnlyTransport = true;
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        var number = Guid.NewGuid().ToString().Replace(".", "-");

        OutboundAddress = new Uri($"{AmazonSnsTransport.SnsProtocol}://receiver-" + number);

        await ReceiverIs(opts =>
        {
            opts.UseAmazonSqsTransportLocally()
                .AutoProvision().AutoPurgeOnStartup();

            opts.ListenToSqsQueue("receiver-" + number).Named("receiver")
                .BufferedInMemory().ReceiveSnsTopicMessage();
        });
        
        await SenderIs(opts =>
        {
            opts.UseAmazonSqsTransportLocally()
                .AutoProvision().AutoPurgeOnStartup();

            opts.ListenToSqsQueue("sender-" + number).ReceiveSnsTopicMessage();
            
            opts.UseAmazonSnsTransportLocally()
                .AutoProvision();

            opts.PublishAllMessages()
                .ToSnsTopic("receiver-" + number)
                .SubscribeSqsQueue("receiver-" + number);
        });
    }
}

public class BufferedSendingAndReceivingCompliance(BufferedComplianceFixture fixture)
    : TransportCompliance<BufferedComplianceFixture>(fixture)
{
}

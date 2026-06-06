using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Wolverine.ComplianceTests.Compliance;
using Wolverine.Runtime;
using Xunit;

namespace Wolverine.Pubsub.Tests;

public class BufferedComplianceFixture : TransportComplianceFixture
{
    public BufferedComplianceFixture() : base(new Uri($"{PubsubTransport.ProtocolName}://wolverine/buffered-receiver"),
        120)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        var id = Guid.NewGuid().ToString();

        OutboundAddress = new Uri($"{PubsubTransport.ProtocolName}://wolverine/buffered-receiver.{id}");

        await SenderIs(opts =>
        {
            opts
                .UsePubsubTesting()
                .AutoProvision()
                .AutoPurgeOnStartup()
                .EnableDeadLettering()
                .EnableSystemEndpoints();
        });

        await ReceiverIs(opts =>
        {
            opts
                .UsePubsubTesting()
                .AutoProvision()
                .AutoPurgeOnStartup()
                .EnableDeadLettering()
                .EnableSystemEndpoints();

            opts
                .ListenToPubsubTopic($"buffered-receiver.{id}")
                .BufferedInMemory();
        });
    }
}

[Collection("acceptance")]
public class BufferedSendingAndReceivingCompliance(BufferedComplianceFixture fixture)
    : TransportCompliance<BufferedComplianceFixture>(fixture)
{
    [Fact]
    public virtual async Task dl_mechanics()
    {
        throwOnAttempt<DivideByZeroException>(1);
        throwOnAttempt<DivideByZeroException>(2);
        throwOnAttempt<DivideByZeroException>(3);

        await shouldMoveToErrorQueueOnAttempt(1);

        var runtime = theReceiver.Services.GetRequiredService<IWolverineRuntime>();
        var transport = runtime.Options.Transports.GetOrCreate<PubsubTransport>();
        var dl = transport.Topics[PubsubTransport.DeadLetterName];

        await dl.InitializeAsync(NullLogger.Instance);

        var pullResponse = await transport.SubscriberApiClient!.PullAsync(
            dl.Server.Subscription.Name,
            1
        );

        pullResponse.ReceivedMessages.ShouldNotBeEmpty();
    }
}
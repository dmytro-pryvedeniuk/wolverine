using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Wolverine.AmazonSqs.Internal;
using Wolverine.ComplianceTests.Compliance;
using Wolverine.Runtime;

namespace Wolverine.AmazonSqs.Tests;

public class BufferedComplianceFixture : TransportComplianceFixture
{
    public BufferedComplianceFixture() : base(new Uri("sqs://receiver"), 120)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        var number = Guid.NewGuid().ToString().Replace(".", "-");

        OutboundAddress = new Uri("sqs://receiver-" + number);

        await SenderIs(opts =>
        {
            opts.UseAmazonSqsTransportLocally()
                .AutoProvision().AutoPurgeOnStartup()
                .EnableSystemQueues();

            opts.ListenToSqsQueue("sender-" + number);
        });

        await ReceiverIs(opts =>
        {
            opts.UseAmazonSqsTransportLocally()
                .AutoProvision().AutoPurgeOnStartup()
                .EnableSystemQueues();

            opts.ListenToSqsQueue("receiver-" + number).Named("receiver").BufferedInMemory();
        });
    }
}

public class BufferedSendingAndReceivingCompliance(BufferedComplianceFixture fixture)
    : TransportCompliance<BufferedComplianceFixture>(fixture)
{
    [Fact]
    public virtual async Task dlq_mechanics()
    {
        throwOnAttempt<DivideByZeroException>(1);
        throwOnAttempt<DivideByZeroException>(2);
        throwOnAttempt<DivideByZeroException>(3);

        await shouldMoveToErrorQueueOnAttempt(1);

        var runtime = theReceiver.Services.GetRequiredService<IWolverineRuntime>();

        var transport = runtime.Options.Transports.GetOrCreate<AmazonSqsTransport>();
        var queue = transport.Queues[AmazonSqsTransport.DeadLetterQueueName];
        await queue.InitializeAsync(NullLogger.Instance);
        var messages = await transport.Client!.ReceiveMessageAsync(queue.QueueUrl);
        messages.Messages.Count.ShouldBeGreaterThan(0);
    }
}
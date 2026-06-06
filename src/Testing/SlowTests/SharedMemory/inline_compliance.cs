using Wolverine.ComplianceTests.Compliance;
using Wolverine.Transports.SharedMemory;
using Xunit;

namespace SlowTests.SharedMemory;

public class InlineSharedMemoryInlineFixture : TransportComplianceFixture
{
    public InlineSharedMemoryInlineFixture() : base(new Uri("shared-memory://receiver"), 5)
    {
        AllLocally = true;
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await SharedMemoryQueueManager.ClearAllAsync();
        
        await ReceiverIs(opts =>
        {
            opts.ListenToSharedMemorySubscription("receiver", "receiver").ProcessInline();
        });

        await SenderIs(opts =>
        {
            opts.ListenToSharedMemorySubscription("sender", "sender");
            opts.PublishAllMessages().ToSharedMemoryTopic("receiver").SendInline();
        });
    }

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await SharedMemoryQueueManager.ClearAllAsync();
    }
}

public class inline_compliance(InlineSharedMemoryInlineFixture fixture)
    : TransportCompliance<InlineSharedMemoryInlineFixture>(fixture);
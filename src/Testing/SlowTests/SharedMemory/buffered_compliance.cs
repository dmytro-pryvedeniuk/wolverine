using Wolverine.ComplianceTests.Compliance;
using Wolverine.Transports.SharedMemory;
using Xunit;

namespace SlowTests.SharedMemory;

public class BufferedSharedMemoryInlineFixture : TransportComplianceFixture
{
    public BufferedSharedMemoryInlineFixture() : base(new Uri("shared-memory://receiver"), 5)
    {
        AllLocally = true;
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await SharedMemoryQueueManager.ClearAllAsync();
        
        await ReceiverIs(opts =>
        {
            opts.ListenToSharedMemorySubscription("receiver", "receiver");
        });

        await SenderIs(opts =>
        {
            opts.ListenToSharedMemorySubscription("sender", "sender");
            opts.PublishAllMessages().ToSharedMemoryTopic("receiver");
        });
    }

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await SharedMemoryQueueManager.ClearAllAsync();
    }
}

public class buffered_compliance(BufferedSharedMemoryInlineFixture fixture)
    : TransportCompliance<BufferedSharedMemoryInlineFixture>(fixture);
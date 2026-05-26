using NSubstitute;
using Wolverine.Runtime;
using Wolverine.Runtime.WorkerQueues;
using Wolverine.Transports.Stub;
using Xunit;

namespace CoreTests.Runtime.WorkerQueues;

public class durable_receiver_disposal
{
    private readonly MockWolverineRuntime _runtime;
    private readonly DurableReceiver _receiver;

    public durable_receiver_disposal()
    {
        _runtime = new MockWolverineRuntime();
        var transport = new StubTransport();
        var endpoint = new StubEndpoint("disposal", transport);
        _receiver = new DurableReceiver(endpoint, _runtime, Substitute.For<IHandlerPipeline>());
    }

    [Fact]
    public void dispose_does_not_throw()
    {
        _receiver.Dispose();
    }

    [Fact]
    public void double_dispose_does_not_throw()
    {
        _receiver.Dispose();
        _receiver.Dispose();
    }

    [Fact]
    public async Task dispose_async_does_not_throw()
    {
        await _receiver.DisposeAsync();
    }

    [Fact]
    public async Task double_dispose_async_does_not_throw()
    {
        await _receiver.DisposeAsync();
        await _receiver.DisposeAsync();
    }

    [Fact]
    public async Task dispose_then_dispose_async_does_not_throw()
    {
        _receiver.Dispose();
        await _receiver.DisposeAsync();
    }

    [Fact]
    public async Task dispose_async_then_dispose_does_not_throw()
    {
        await _receiver.DisposeAsync();
        _receiver.Dispose();
    }
}

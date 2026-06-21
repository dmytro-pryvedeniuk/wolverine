using JasperFx.Blocks;
using JasperFx.Core.Reflection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Wolverine.Configuration;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace Wolverine.RabbitMQ.Internal;

internal class RabbitMqInteropFriendlyCallback : IChannelCallback, ISupportDeadLetterQueue
{
    private readonly IChannelCallback _inner;
    private readonly RetryBlock<Envelope> _sendBlock;


    public RabbitMqInteropFriendlyCallback(RabbitMqTransport transport, RabbitMqQueue deadLetterQueue,
        IWolverineRuntime runtime)
    {
        _inner = transport.Callback!;
        var sender = deadLetterQueue.ResolveSender(runtime);

        _sendBlock =
            new RetryBlock<Envelope>((e, _) => sender.SendAsync(e).AsTask(), runtime.Logger, runtime.Cancellation);
    }

    public IHandlerPipeline? Pipeline => _inner.Pipeline;

    public ValueTask CompleteAsync(Envelope envelope)
    {
        return _inner.CompleteAsync(envelope);
    }

    public ValueTask DeferAsync(Envelope envelope)
    {
        return _inner.DeferAsync(envelope);
    }

    public async Task MoveToErrorsAsync(Envelope envelope, Exception exception)
    {
        DeadLetterQueueConstants.StampFailureMetadata(envelope, exception);
        await _sendBlock.PostAsync(envelope);
    }

    public bool NativeDeadLetterQueueEnabled => true;
}

internal class RabbitMqListener : RabbitMqChannelAgent, IListener, ISupportDeadLetterQueue, ISupportMultipleConsumers
{
    private readonly IChannelCallback _callback;
    private readonly CancellationToken _cancellation = CancellationToken.None;
    private readonly ISupportDeadLetterQueue _deadLetterQueueCallback;
    private readonly IReceiver _receiver;
    private readonly IWolverineRuntime _runtime;
    private readonly Lazy<ISender> _sender;
    private readonly RabbitMqTransport _transport;
    private volatile WorkerQueueMessageConsumer? _consumer;
    private string? _consumerId;
    private volatile bool _disposed;

    public RabbitMqListener(IWolverineRuntime runtime,
        RabbitMqQueue queue, RabbitMqTransport transport, IReceiver receiver) : base(
        transport.UseSenderConnectionOnly ? transport.SendingConnection : transport.ListeningConnection,
        runtime.LoggerFactory.CreateLogger<RabbitMqListener>())
    {
        Queue = queue;
        Address = queue.Uri;
        ConsumerAddress = Address;

        _sender = new Lazy<ISender>(() => Queue.ResolveSender(runtime));

        _runtime = runtime;
        _transport = transport;
        _receiver = receiver ?? throw new ArgumentNullException(nameof(receiver));

        var useEnhancedOrInterop = Queue.DeadLetterQueue != null &&
                                    (Queue.DeadLetterQueue.Mode == DeadLetterQueueMode.InteropFriendly ||
                                     _transport.UseEnhancedDeadLettering);

        _callback = useEnhancedOrInterop
            ? new RabbitMqInteropFriendlyCallback(_transport, _transport.Queues[Queue.DeadLetterQueue!.QueueName],
                _runtime)
            : _transport.Callback!;

        _deadLetterQueueCallback = _callback.As<ISupportDeadLetterQueue>();
        // Need to disable this if using WolverineStorage
        NativeDeadLetterQueueEnabled = queue.DeadLetterQueue != null &&
                                       queue.DeadLetterQueue.Mode != DeadLetterQueueMode.WolverineStorage;
    }

    public RabbitMqQueue Queue { get; }

    public async ValueTask StopAsync()
    {
        var consumer = _consumer;
        if (consumer == null)
            return;
        consumer.Latch();

        try
        {
            await RunWithLockAsync(consumer.Channel, async ch =>
            {
                foreach (var consumerTag in consumer.ConsumerTags)
                    await ch.BasicCancelAsync(consumerTag, noWait: false, default);
            });
        }
        catch (Exception e) when (e is ObjectDisposedException or AlreadyClosedException)
        {
            // Shutting down — nothing to cancel
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        _consumer?.Dispose();
        await base.DisposeAsync();
        
        // Don't dispose _sender.Value — it's a shared sender cached on
        // RabbitMqQueue and reused across listener pause/restart cycles.
    }

    public async Task<bool> TryRequeueAsync(Envelope envelope)
    {
        if (envelope is not RabbitMqEnvelope e)
        {
            return false;
        }

        await e.RabbitMqListener.RequeueAsync(e);
        return true;
    }

    public Uri Address { get; }

    public IHandlerPipeline? Pipeline => _receiver.Pipeline;

    public ValueTask CompleteAsync(Envelope envelope)
    {
        return _callback.CompleteAsync(envelope);
    }

    public ValueTask DeferAsync(Envelope envelope)
    {
        return _callback.DeferAsync(envelope);
    }

    public Task MoveToErrorsAsync(Envelope envelope, Exception exception)
    {
        return _deadLetterQueueCallback.MoveToErrorsAsync(envelope, exception);
    }

    public bool NativeDeadLetterQueueEnabled { get; }

    public string? ConsumerId
    {
        get => _consumerId;
        set
        {
            _consumerId = value;

            if (value == null)
            {
                ConsumerAddress = Address;
            }
            else
            {
                ConsumerAddress = new Uri($"{Address}?consumer={_consumerId}");
            }
        }
    }

    public Uri BaseAddress => Queue.Uri;
    public Uri ConsumerAddress { get; private set; }

    public Task CreateAsync()
    {
        return RunWithLockAsync(_ => ValueTask.CompletedTask);
    }

    internal async ValueTask CreateInternalAsync(IChannel ch)
    {
        if (Queue.AutoDelete || _transport.AutoProvision)
        {
            await Queue.DeclareAsync(ch, Logger);

            if (Queue.DeadLetterQueue != null && Queue.DeadLetterQueue.Mode != DeadLetterQueueMode.WolverineStorage)
            {
                var dlq = _transport.Queues[Queue.DeadLetterQueue.QueueName];
                await dlq.DeclareAsync(ch, Logger);
            }
        }

        try
        {
            var result = await ch.QueueDeclarePassiveAsync(Queue.QueueName, _cancellation);
            if (Queue.Role == EndpointRole.Application)
            {
                Logger.LogInformation("{Count} messages in queue {QueueName} at listening start up time",
                    result.MessageCount, Queue.QueueName);
            }
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Unable to check the queued count for {QueueName}", Queue.QueueName);
        }

        var mapper = Queue.BuildMapper(_runtime);

        _consumer = new WorkerQueueMessageConsumer(ch, _receiver, Logger, this, mapper, Address, _cancellation);

        await ch.BasicQosAsync(0, Queue.PreFetchCount, false, _cancellation);
        await ch.BasicConsumeAsync(Queue.QueueName, false,
            _transport.ConnectionFactory?.ClientProvidedName ?? _runtime.Options.ServiceName, Queue.ConsumerArguments, _consumer,
            _runtime.Cancellation);

        if (_transport.AutoPingListeners)
        {
            // This is trying to be a forcing function to make the channel really connect
            var ping = Envelope.ForPing(Address);
            await _sender.Value.SendAsync(ping);
        }
    }

    internal override async Task ReconnectedAsync()
    {
        // Cancel existing consumers on the old channel
        await StopAsync();

        // Atomically replace the channel and set up the new consumer
        await ReplaceChannelAndSetupAsync(CreateInternalAsync);
    }

    protected override async ValueTask OnChannelRestartedAsync(IChannel channel)
    {
        _consumer?.Dispose();
        await CreateInternalAsync(channel);
    }

    public override string ToString()
    {
        return $"RabbitMqListener: {Address}";
    }

    public async ValueTask RequeueAsync(RabbitMqEnvelope envelope)
    {
        if (!envelope.Acknowledged && _consumer?.Channel is { } channel)
        {
            try
            {
                await RunWithLockAsync(channel,
                    ch => ch.BasicNackAsync(envelope.DeliveryTag, multiple: false, requeue: false, _cancellation));
            }
            catch (Exception ex) when (ex is ObjectDisposedException or AlreadyClosedException)
            {
                Logger.LogInformation("Channel unavailable while nacking for requeue, sending new copy anyway");
            }
        }

        Logger.LogDebug("RequeueAsync: sending new copy for deliveryTag={DeliveryTag}", envelope.DeliveryTag);
        await _sender.Value.SendAsync(envelope);
        Logger.LogDebug("RequeueAsync: new copy sent for deliveryTag={DeliveryTag}", envelope.DeliveryTag);
    }

    internal Task NackDeliveryAsync(ulong deliveryTag)
    {
        if (_consumer?.Channel is not { } channel)
            return Task.CompletedTask;

        return RunWithLockAsync(channel, async ch =>
        {
            await ch.BasicNackAsync(deliveryTag, multiple: false, requeue: false, _cancellation);
            Logger.LogDebug("NackDeliveryAsync succeeded for deliveryTag={DeliveryTag}", deliveryTag);
        });
    }

    public Task CompleteAsync(ulong deliveryTag)
    {
        if (_consumer?.Channel is not { } channel)
            return Task.CompletedTask;

        return RunWithLockAsync(channel, async ch =>
        {
            await ch.BasicAckAsync(deliveryTag, multiple: false, _cancellation);
            Logger.LogDebug("CompleteAsync succeeded for deliveryTag={DeliveryTag}", deliveryTag);
        });
    }
}

using JasperFx.Blocks;
using JasperFx.Core.Reflection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
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
    private WorkerQueueMessageConsumer? _consumer;
    private string? _consumerId;

    public RabbitMqListener(IWolverineRuntime runtime,
        RabbitMqQueue queue, RabbitMqTransport transport, IReceiver receiver) : base(
        transport.UseSenderConnectionOnly ? transport.SendingConnection : transport.ListeningConnection,
        runtime.LoggerFactory.CreateLogger<RabbitMqListener>())
    {
        Queue = queue;
        Address = queue.Uri;
        ConsumerAddress = Address;

        _sender = new Lazy<ISender>(() => Queue.ResolveSender(runtime));
        _cancellation.Register(() => { _ = teardownChannel(); });

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
        {
            return;
        }

        // Latch the consumer BEFORE sending BasicCancel so that any in-flight
        // AMQP deliveries that arrive after this point are reliably re-queued via
        // the guard path (which always falls through to the sender connection).
        // Without this, a late delivery could enter the normal path and be posted
        // to a completed BufferedReceiver._receivingBlock, where it's silently dropped.
        consumer.Dispose();
        _consumer = null;

        foreach (var consumerTag in consumer.ConsumerTags)
        {
            await RunOnChannelAsync((ch, ct) => new ValueTask(ch.BasicCancelAsync(consumerTag, true, ct)));
        }
    }

    public override async ValueTask DisposeAsync()
    {
        _consumer?.Dispose();
        _consumer = null;

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

    public async Task CreateAsync()
    {
        await EnsureInitiated();

        if (!await RunOnChannelAsync(async (ch, ct) =>
            {
                await StartConsumingAsync(ch);
            }))
        {
            throw new InvalidOperationException($"Cannot start listener {Address} — channel not available");
        }

        if (_transport.AutoPingListeners)
        {
            // This is trying to be a forcing function to make the channel really connect
            var ping = Envelope.ForPing(Address);
            await _sender.Value.SendAsync(ping);
        }
    }

    /// <summary>
    /// Register the consumer on the given channel. Shared between initial
    /// startup (<see cref="CreateAsync"/>) and channel recovery
    /// (<see cref="RecoverChannelAsync"/>).
    /// </summary>
    private async Task StartConsumingAsync(IChannel ch)
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
    }

    protected override async Task RecoverChannelAsync()
    {
        await base.RecoverChannelAsync();

        // Re-register the consumer on the new channel.
        await RunOnChannelAsync((ch, ct) => new ValueTask(StartConsumingAsync(ch)));
    }

    internal override async Task ReconnectedAsync()
    {
        await StopAsync();
        await teardownChannel();
        await CreateAsync();

        await base.ReconnectedAsync();
    }

    public override string ToString()
    {
        return $"RabbitMqListener: {Address}";
    }

    public async ValueTask RequeueAsync(RabbitMqEnvelope envelope)
    {
        // Used by the normal Defer/requeue path (RequeueContinuation).
        // NACK the original delivery (best-effort) then publish a fresh copy via sender.
        // If the NACK succeeds but the send fails, the message is lost - but the normal
        // path runs on a healthy channel, so this failure is extremely unlikely.
        if (!envelope.Acknowledged)
        {
            await RunOnChannelAsync((ch, ct) =>
                ch.BasicNackAsync(envelope.DeliveryTag, false, false, ct));
        }

        await _sender.Value.SendAsync(envelope);
    }

    /// <summary>
    /// Publish a fresh copy via the sender connection WITHOUT NACK'ing the original.
    /// Used when the listener channel is dying and NACK frames may be silently dropped.
    /// The original un-acked delivery will be auto-requeued by RMQ when the
    /// consumer channel fully closes.
    /// </summary>
    public async ValueTask RequeueViaSenderAsync(RabbitMqEnvelope envelope)
    {
        await _sender.Value.SendAsync(envelope);
    }

    public async Task CompleteAsync(ulong deliveryTag)
    {
        await RunOnChannelAsync((ch, ct) =>
            ch.BasicAckAsync(deliveryTag, true, ct));
    }
}

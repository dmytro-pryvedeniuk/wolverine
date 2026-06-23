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

        // Latch outside the lock so any messages delivered after this point
        // are rejected (Nack with requeue) and redelivered to the new consumer.
        consumer.Latch();

        var channel = consumer.Channel;
        if (!IsConnected)
            return;

        await RunWithLockAsync(channel, async ch =>
        {
            foreach (var consumerTag in consumer.ConsumerTags)
                await channel.BasicCancelAsync(consumerTag, noWait: true, default);
        });
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
        await RunWithLockAsync(CreateInternalAsync);

        if (_transport.AutoPingListeners)
        {
            // This is trying to be a forcing function to make the channel really connect
            var ping = Envelope.ForPing(Address);
            await _sender.Value.SendAsync(ping);
        }
    }

    private async ValueTask CreateInternalAsync(IChannel channel)
    {
        if (Queue.AutoDelete || _transport.AutoProvision)
        {
            await Queue.DeclareAsync(channel, Logger);

            if (Queue.DeadLetterQueue != null && Queue.DeadLetterQueue.Mode != DeadLetterQueueMode.WolverineStorage)
            {
                var dlq = _transport.Queues[Queue.DeadLetterQueue.QueueName];
                await dlq.DeclareAsync(channel, Logger);
            }
        }

        try
        {
            var result = await channel.QueueDeclarePassiveAsync(Queue.QueueName, _cancellation);
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

        _consumer = new WorkerQueueMessageConsumer(channel, _receiver, Logger, this, mapper, Address, _cancellation);

        await channel.BasicQosAsync(0, Queue.PreFetchCount, false, _cancellation);
        await channel.BasicConsumeAsync(Queue.QueueName, false,
            _transport.ConnectionFactory?.ClientProvidedName ?? _runtime.Options.ServiceName, Queue.ConsumerArguments, _consumer,
            _runtime.Cancellation);
    }

    internal override async Task ReconnectedAsync()
    {
        await StopAsync();
        await base.ReconnectedAsync();
        await RunWithLockAsync(CreateInternalAsync);

        if (_transport.AutoPingListeners)
        {
            // This is trying to be a forcing function to make the channel really connect
            var ping = Envelope.ForPing(Address);
            await _sender.Value.SendAsync(ping);
        }
    }

    public override string ToString()
    {
        return $"RabbitMqListener: {Address}";
    }

    public async ValueTask RequeueAsync(RabbitMqEnvelope envelope)
    {
        if (!envelope.Acknowledged)
            await NackDeliveryAsync(envelope.DeliveryTag, envelope.DeliveryChannel, _cancellation);

        await _sender.Value.SendAsync(envelope);
    }

    public Task CompleteAsync(ulong deliveryTag, IChannel deliveryChannel)
    {
        return RunWithLockAsync(deliveryChannel, async ch =>
        {
            await ch.BasicAckAsync(deliveryTag, multiple: true, _cancellation);
            Logger.LogDebug("CompleteAsync succeeded for deliveryTag={DeliveryTag}", deliveryTag);
        });
    }

    internal Task NackDeliveryAsync(ulong deliveryTag, IChannel deliveryChannel, CancellationToken cancellationToken)
    {
        return RunWithLockAsync(deliveryChannel, async ch =>
        {
            await ch.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
            Logger.LogDebug("NackDeliveryAsync succeeded for deliveryTag={DeliveryTag}", deliveryTag);
        });
    }

    internal Task RejectDeliveryAsync(ulong deliveryTag, IChannel deliveryChannel, CancellationToken cancellationToken)
    {
        return RunWithLockAsync(deliveryChannel, async ch =>
        {
            await ch.BasicRejectAsync(deliveryTag, requeue: true, cancellationToken);
            Logger.LogDebug("RejectDeliveryAsync succeeded for deliveryTag={DeliveryTag}", deliveryTag);
        });
    }
}

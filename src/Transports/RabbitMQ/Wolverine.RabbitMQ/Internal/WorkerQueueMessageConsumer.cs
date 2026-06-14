using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Wolverine.Transports;

namespace Wolverine.RabbitMQ.Internal;

internal class WorkerQueueMessageConsumer : AsyncDefaultBasicConsumer, IDisposable
{
    private readonly Uri _address;
    private readonly CancellationToken _cancellation;
    private readonly RabbitMqListener _listener;
    private readonly ILogger _logger;
    private readonly IRabbitMqEnvelopeMapper _mapper;
    private readonly IReceiver _workerQueue;
    private volatile bool _latched;

    public WorkerQueueMessageConsumer(IChannel channel, IReceiver workerQueue, ILogger logger,
        RabbitMqListener listener,
        IRabbitMqEnvelopeMapper mapper, Uri address, CancellationToken cancellation) : base(channel)
    {
        _workerQueue = workerQueue;
        _logger = logger;
        _listener = listener;
        _mapper = mapper;
        _address = address;
        _cancellation = cancellation;
    }

    public void Dispose()
    {
        _latched = true;
    }

    //TODO do something with the token passed in here
    public override async Task HandleBasicDeliverAsync(string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
        string routingKey, IReadOnlyBasicProperties properties, ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken = new())
    {
        if (IsListenerChannelNotReady())
        {
            // Listener channel may not be ready — BasicNack frames may be silently dropped.
            // Try NACK first; fall back to sender connection if NACK fails.
            try
            {
                var guardEnvelope = MapIncomingToEnvelope(deliveryTag, properties, body);
                await NackOrRequeueAsync(deliveryTag, guardEnvelope, requeue: true);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to requeue message (dt={DeliveryTag}) during guard", deliveryTag);
            }
            return;
        }

        RabbitMqEnvelope envelope;
        try
        {
            envelope = MapIncomingToEnvelope(deliveryTag, properties, body);
        }
        catch (Exception ex)
        {
            await HandleUnmappableMessageAsync(ex, deliveryTag, properties, body);
            return;
        }

        if (envelope.IsPing())
        {
            await Channel.BasicAckAsync(deliveryTag, false, _cancellation);
            return;
        }

        // TOCTOU guard: IsListenerChannelNotReady() may have become true between
        // the first check and this point (e.g. during StopAsync's consumer.Dispose()).
        // The receiver's Block may already be Complete'd — posting would silently
        // drop the message.
        if (IsListenerChannelNotReady())
        {
            await NackOrRequeueAsync(deliveryTag, envelope, requeue: true);
            return;
        }

        try
        {
            await _workerQueue.ReceivedAsync(_listener, envelope);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failure to receive an incoming message with {Id}, trying to 'Nack' the message", envelope.Id);
            await NackOrRequeueAsync(deliveryTag, envelope, requeue: true);
        }
    }

    private async Task HandleUnmappableMessageAsync(
        Exception exception, ulong deliveryTag, 
        IReadOnlyBasicProperties properties, ReadOnlyMemory<byte> body)
    {
        _logger.LogError(exception, "Error trying to map an incoming RabbitMQ message {MessageId} to an Envelope",
            properties.MessageId);

        var envelope = new RabbitMqEnvelope(_listener, deliveryTag)
        {
            Id = Guid.NewGuid(),
            Data = body.ToArray()
        };

        try
        {
            if (_workerQueue is ISupportDeadLetterQueue dlq)
            {
                await dlq.MoveToErrorsAsync(envelope, exception);
                return;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to move un-mappable RabbitMQ message {MessageId} to the dead-letter store; " +
                "falling back to broker DLX",
                properties.MessageId);
        }

        await NackOrRequeueAsync(deliveryTag, envelope, requeue: false);
    }

    private RabbitMqEnvelope MapIncomingToEnvelope(
        ulong deliveryTag, IReadOnlyBasicProperties properties, ReadOnlyMemory<byte> body)
    {
        var envelope = new RabbitMqEnvelope(_listener, deliveryTag)
        {
            Data = body.ToArray()
        };
        _mapper.MapIncomingToEnvelope(envelope, properties);
        return envelope;
    }

    private bool IsListenerChannelNotReady() =>
        _latched || _cancellation.IsCancellationRequested || !_listener.IsConnected;

    private async Task NackOrRequeueAsync(ulong deliveryTag, RabbitMqEnvelope envelope, bool requeue)
    {
        try
        {
            await Channel.BasicNackAsync(deliveryTag, multiple: false, requeue, _cancellation);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to Nack dt={DeliveryTag}", deliveryTag);

            if (!requeue)
                return;

            try
            {
                await _listener.RequeueViaSenderAsync(envelope);
            }
            catch (Exception requeueEx)
            {
                _logger.LogError(requeueEx,
                    "Failed to requeue envelope (dt={DeliveryTag}) via sender after failed NACK", deliveryTag);
            }
        }
    }
}
using JasperFx.Blocks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Exceptions;
using Wolverine.Runtime;
using Wolverine.Transports;

namespace Wolverine.RabbitMQ.Internal;

internal class RabbitMqChannelCallback : IChannelCallback, IDisposable, ISupportDeadLetterQueue
{
    private readonly RetryBlock<RabbitMqEnvelope> _deadLetterQueue;

    internal RabbitMqChannelCallback(ILogger logger, CancellationToken cancellationToken)
    {
        Logger = logger;
        Complete = new RetryBlock<RabbitMqEnvelope>(async (e, _) =>
        {
            try
            {
                await e.CompleteAsync();
            }
            catch (Exception ex) when (ex is ObjectDisposedException or AlreadyClosedException)
            {
                logger.LogInformation("Channel unavailable, discarding the envelope");
            }
        }, logger, cancellationToken);

        Defer = new RetryBlock<RabbitMqEnvelope>((e, _) => e.DeferAsync().AsTask(), logger, cancellationToken);
        _deadLetterQueue = new RetryBlock<RabbitMqEnvelope>(moveToErrorQueueAsync, logger, cancellationToken);
    }

    public ILogger Logger { get; }

    public RetryBlock<RabbitMqEnvelope> Complete { get; }

    public RetryBlock<RabbitMqEnvelope> Defer { get; }

    public IHandlerPipeline? Pipeline => null;

    public ValueTask CompleteAsync(Envelope envelope)
    {
        if (envelope is RabbitMqEnvelope e)
        {
            return new ValueTask(Complete.PostAsync(e));
        }

        Logger.LogDebug(
            "Attempting to complete and ack a message to a Rabbit MQ queue, but envelope {Id} is not a RabbitMqEnvelope",
            envelope.Id);

        return ValueTask.CompletedTask;
    }

    public ValueTask DeferAsync(Envelope envelope)
    {
        if (envelope is RabbitMqEnvelope e)
        {
            return new ValueTask(Defer.PostAsync(e));
        }

        Logger.LogDebug(
            "Attempting to complete and nack a message to a Rabbit MQ queue, but envelope {Id} is not a RabbitMqEnvelope",
            envelope.Id);

        return ValueTask.CompletedTask;
    }

    public virtual void Dispose()
    {
        Complete.Dispose();
        Defer.Dispose();
        _deadLetterQueue.Dispose();
    }

    public Task MoveToErrorsAsync(Envelope envelope, Exception exception)
    {
        if (envelope is RabbitMqEnvelope e)
        {
            return _deadLetterQueue.PostAsync(e);
        }

        Logger.LogDebug(
            "Attempting to move a message to a Rabbit MQ dead letter queue, but envelope {Id} is not a RabbitMqEnvelope",
            envelope.Id);

        return Task.CompletedTask;
    }

    public bool NativeDeadLetterQueueEnabled => true;

    private async Task moveToErrorQueueAsync(RabbitMqEnvelope envelope, CancellationToken token)
    {
        try
        {
            if (envelope.RabbitMqListener.IsConnected)
            {
                // Mark as acknowledged before the NACK so that any subsequent
                // CompleteAsync() call is a no-op (prevents double ack/nack)
                envelope.Acknowledged = true;
                envelope.HasBeenAcked = true;
                await envelope.RabbitMqListener.NackDeliveryAsync(envelope.DeliveryTag);
            }
        }
        catch (Exception ex) when (ex is ObjectDisposedException or AlreadyClosedException)
        {
            Logger.LogInformation("Channel unavailable, discarding the envelope");
        }
    }
}
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Wolverine.RabbitMQ.Internal;

/// <summary>
/// Base class for Rabbit MQ listeners and senders
/// </summary>
internal abstract class RabbitMqChannelAgent : IAsyncDisposable
{
    private readonly ConnectionMonitor _monitor;
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly SemaphoreSlim _locker = new(1, 1);
    private IChannel? _channel;
    private bool _disposed;

    protected RabbitMqChannelAgent(ConnectionMonitor monitor, ILogger logger)
    {
        _monitor = monitor;
        Logger = logger;
        monitor.Track(this);
    }

    public ILogger Logger { get; }

    internal AgentState State { get; private set; } = AgentState.Disconnected;
    internal bool IsConnected => State == AgentState.Connected;

    /// <summary>
    /// Execute an action on the current channel if one is available.
    /// </summary>
    internal async Task<bool> RunOnChannelAsync(
        Func<IChannel, CancellationToken, ValueTask> action,
        CancellationToken ct = default)
    {
        var channel = _channel;
        if (channel is null)
            return false;

        try
        {
            await action(channel, ct);
            return true;
        }
        catch (AlreadyClosedException)
        {
            // Channel was closed concurrently - broker handles redelivery
            return false;
        }
        catch (ObjectDisposedException)
        {
            // Channel was disposed concurrently
            return false;
        }
        catch (OperationCanceledException)
        {
            // Agent is shutting down
            return false;
        }
    }

    public virtual async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Signal the error-recovery callback to bail out before we tear down.
        await _disposeCts.CancelAsync();

        _monitor.Remove(this);
        await teardownChannel();

        _disposeCts.Dispose();
        _locker.Dispose();
    }

    internal async Task EnsureInitiated()
    {
        if (_disposed) return;
        if (_channel is not null)
            return;

        if (!await TryToWaitAsync(_locker, _disposeCts.Token))
            return;

        try
        {
            if (_channel is not null)
                return;

            await startNewChannel();
            State = AgentState.Connected;
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Error trying to start a new Rabbit MQ channel for {Endpoint}", this);
        }
        finally
        {
            TryToUnlock(_locker);
        }
    }

    protected async Task startNewChannel()
    {
        var channel = await _monitor.CreateChannelAsync();
        _channel = channel;

        channel.CallbackExceptionAsync += HandleChannelExceptionAsync;
        channel.ChannelShutdownAsync += HandleChannelShutdownAsync;

        Logger.LogInformation("Opened a new channel for Wolverine endpoint {Endpoint}", this);
    }

    private async Task HandleChannelShutdownAsync(object? sender, ShutdownEventArgs e)
    {
        State = AgentState.Disconnected;

        if (e.Initiator == ShutdownInitiator.Application) return;

        if (e.Exception != null)
        {
            Logger.LogError(e.Exception,
                "Unexpected channel shutdown for Rabbit MQ. Wolverine will attempt to restart...");
        }

        await RecoverChannelAsync();
    }

    internal virtual Task ReconnectedAsync()
    {
        State = AgentState.Connected;
        return Task.CompletedTask;
    }

    protected async Task teardownChannel()
    {
        var channel = _channel;
        if (channel != null)
        {
            channel.CallbackExceptionAsync -= HandleChannelExceptionAsync;
            channel.ChannelShutdownAsync -= HandleChannelShutdownAsync;
            try
            {
                await channel.CloseAsync();
                await channel.AbortAsync();
            }
            catch (OperationCanceledException)
            {
                // Channel teardown was cancelled during shutdown race.
            }
            channel.Dispose();
        }

        _channel = null;

        State = AgentState.Disconnected;
    }

    private async Task HandleChannelExceptionAsync(object? sender, CallbackExceptionEventArgs args)
    {
        Logger.LogError(args.Exception, "Callback error in Rabbit Mq agent. Attempting to restart the channel");

        await RecoverChannelAsync();
    }

    /// <summary>
    /// Recover from a channel failure: tear down the old channel and start a new one.
    /// Subclasses (e.g. <see cref="RabbitMqListener"/>) can override to also
    /// re-register any consumer or other per-channel state on the new channel.
    /// </summary>
    protected virtual async Task RecoverChannelAsync()
    {
        if (!await TryToWaitAsync(_locker, _disposeCts.Token))
            return;

        try
        {
            await teardownChannel();
            await startNewChannel();
            State = AgentState.Connected;
            Logger.LogInformation("Restarted the Rabbit MQ channel");
        }
        catch (OperationCanceledException) { }
        catch (Exception e)
        {
            Logger.LogError(e, "Error when trying to restart Rabbit MQ channel for {Endpoint}", this);
        }
        finally
        {
            TryToUnlock(_locker);
        }
    }

    private static async Task<bool> TryToWaitAsync(
        SemaphoreSlim locker, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return false;
        try
        {
            await locker.WaitAsync(cancellationToken);
            return true;
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }

        return false;
    }

    private static void TryToUnlock(SemaphoreSlim locker)
    {
        try
        {
            locker.Release();
        }
        catch (ObjectDisposedException) { }
    }
}
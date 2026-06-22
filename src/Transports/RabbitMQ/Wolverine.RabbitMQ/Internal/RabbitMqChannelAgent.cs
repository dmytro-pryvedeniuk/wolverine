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
    private readonly SemaphoreSlim Locker = new(1, 1);
    private IChannel? _channel;
    private volatile bool _disposed;
    private volatile AgentState _state = AgentState.Disconnected;

    protected RabbitMqChannelAgent(ConnectionMonitor monitor,
        ILogger logger)
    {
        _monitor = monitor;
        Logger = logger;
        monitor.Track(this);
    }

    public ILogger Logger { get; }

    internal bool IsConnected => _state == AgentState.Connected;

    /// <summary>
    /// Execute action on the channel thread-safely.
    /// </summary>
    /// <param name="action">The operation to execute.</param>
    /// <exception cref="ObjectDisposedException">Agent disposed.</exception>
    /// <exception cref="AlreadyClosedException">Channel closed by server.</exception>
    public async Task RunWithLockAsync(Func<IChannel, ValueTask> action)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await Locker.WaitAsync();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_channel is null)
            {
                _channel = await StartNewChannelAsync();
                _state = AgentState.Connected;
            }

            await action(_channel);
        }
        finally
        {
            Locker.Release();
        }
    }

    /// <summary>Execute action on a consumer channel thread-safely.
    /// Verifies the channel is still current before executing.</summary>
    /// <param name="consumerChannel">The channel that received the delivery. Must not be null.</param>
    /// <param name="action">The operation to execute.</param>
    /// <exception cref="ArgumentNullException"><paramref name="consumerChannel"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">Agent disposed.</exception>
    /// <exception cref="AlreadyClosedException">Channel closed by server or channel replaced.</exception>
    public async Task RunWithLockAsync(IChannel consumerChannel, Func<IChannel, ValueTask> action)
    {
        ArgumentNullException.ThrowIfNull(consumerChannel);
        ObjectDisposedException.ThrowIf(_disposed, this);

        await Locker.WaitAsync();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!ReferenceEquals(_channel, consumerChannel))
            {
                Logger.LogWarning(
                    "Consumer channel rejected for {Agent}: channel was replaced", this);
                throw new AlreadyClosedException(
                    new ShutdownEventArgs(ShutdownInitiator.Application, Constants.PreconditionFailed, "Consumer channel is not valid anymore."));
            }

            await action(consumerChannel);
        }
        finally
        {
            Locker.Release();
        }
    }

    public virtual async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        _monitor.Remove(this);

        await Locker.WaitAsync();
        try
        {
            await TeardownChannelAsync(_channel);
            _channel = null;
            _state = AgentState.Disconnected;
        }
        finally
        {
            Locker.Release();
        }

        // Intentionally NOT calling Locker.Dispose() — rapid pause/restart cycles
        // can have an in-flight WaitAsync/Release race with disposal, which would
        // throw ObjectDisposedException. The kernel handle is reclaimed by the
        // SemaphoreSlim finalizer. See #3132.
    }

    private async Task<IChannel> StartNewChannelAsync()
    {
        var channel = await _monitor.CreateChannelAsync();

        channel.CallbackExceptionAsync += HandleChannelExceptionAsync;
        channel.ChannelShutdownAsync += HandleChannelShutdownAsync;
        
        Logger.LogInformation("Opened a new channel for Wolverine endpoint {Endpoint}", this);

        return channel;
    }

    private Task HandleChannelExceptionAsync(object? sender, CallbackExceptionEventArgs args)
    {
        Logger.LogError(args.Exception, "Callback error in Rabbit Mq agent. Attempting to restart the channel");

        _ = Task.Run(async () =>
        {
            IChannel? oldChannel = sender as IChannel;

            await Locker.WaitAsync();
            try
            {
                if (ReferenceEquals(_channel, sender))
                {
                    // Try to restart the connection if sender is still the current channel
                    _channel = await StartNewChannelAsync();
                    _state = AgentState.Connected;
                    Logger.LogInformation("Restarted the Rabbit MQ channel");
                }
            }
            finally
            {
                Locker.Release();
            }

            // Tear down the old channel outside the lock
            await TeardownChannelAsync(oldChannel);
        });

        return Task.CompletedTask;
    }

    private async Task HandleChannelShutdownAsync(object? sender, ShutdownEventArgs e)
    {
        await Locker.WaitAsync();
        try
        {
            if (ReferenceEquals(_channel, sender))
            {
                _channel = null;
                _state = AgentState.Disconnected;
            }
        }
        finally
        {
            Locker.Release();
        }

        if (e.Initiator == ShutdownInitiator.Application)
            return;

        if (e.Exception != null)
        {
            Logger.LogError(e.Exception,
                "Unexpected channel shutdown for Rabbit MQ. Wolverine will attempt to restart...");
        }

        return;
    }

    internal virtual async Task ReconnectedAsync()
    {
        await Locker.WaitAsync();
        try
        {
            await TeardownChannelAsync(_channel);
            _channel = await StartNewChannelAsync();
            _state = AgentState.Connected;
        }
        finally
        {
            Locker.Release();
        }
    }

    private async Task TeardownChannelAsync(IChannel? channel)
    {
        if (channel == null)
            return;

        channel.ChannelShutdownAsync -= HandleChannelShutdownAsync;
        channel.CallbackExceptionAsync -= HandleChannelExceptionAsync;

        try
        {
            await channel.AbortAsync();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error closing channel");
        }

        channel.Dispose();
    }
}
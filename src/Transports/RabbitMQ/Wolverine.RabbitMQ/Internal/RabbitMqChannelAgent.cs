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
    private volatile bool _disposed;
    private volatile AgentState _state = AgentState.Disconnected;
    private IChannel? _channel;

    protected RabbitMqChannelAgent(ConnectionMonitor monitor,
        ILogger logger)
    {
        _monitor = monitor;
        Logger = logger;
        monitor.Track(this);
    }

    public ILogger Logger { get; }

    internal bool IsConnected => _state == AgentState.Connected;

    public virtual async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        _monitor.Remove(this);


        await Locker.WaitAsync();
        try
        {
            await TeardownChannelAsync();
        }
        finally
        {
            Locker.Release();
            // Intentionally NOT calling Locker.Dispose() - rapid pause/restart cycles
            // can have an in-flight WaitAsync/Release race with disposal, which would
            // throw ObjectDisposedException. The kernel handle is reclaimed by the
            // SemaphoreSlim finalizer. See #3132.
        }
    }

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

            if (_channel == null)
                await StartNewChannelAsync();

            try
            {
                await action(_channel!);
            }
            catch (Exception ex) when (ex is ObjectDisposedException or AlreadyClosedException)
            {
                Logger.LogWarning("Channel operation failed with {ExceptionType}, replacing channel and retrying", ex.GetType().Name);
                _channel = null;
                _state = AgentState.Disconnected;
                await StartNewChannelAsync();
                await action(_channel!);
            }
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
    /// <exception cref="ObjectDisposedException">Agent disposed or channel replaced.</exception>
    /// <exception cref="AlreadyClosedException">Channel closed by server.</exception>
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
                throw new ObjectDisposedException(nameof(IChannel),
                    "Consumer channel is not valid anymore.");
            }

            await action(consumerChannel);
        }
        finally
        {
            Locker.Release();
        }
    }

    private async Task StartNewChannelAsync()
    {
        _channel = await _monitor.CreateChannelAsync();
        _channel.CallbackExceptionAsync += HandleChannelExceptionAsync;
        _channel.ChannelShutdownAsync += HandleChannelShutdownAsync;

        _state = AgentState.Connected;

        Logger.LogInformation("Opened a new channel for Wolverine endpoint {Endpoint}", this);
    }

    private Task HandleChannelExceptionAsync(object? sender, CallbackExceptionEventArgs args)
    {
        Logger.LogError(args.Exception, "Callback error in Rabbit Mq agent. Dropping channel");

        if (sender is IChannel channel)
            _ = Task.Run(async () => await ShutdownChannelAsync(channel));

        return Task.CompletedTask;
    }

    private async Task HandleChannelShutdownAsync(object? sender, ShutdownEventArgs e)
    {
        if (sender is IChannel channel)
            _ = Task.Run(async () => await ShutdownChannelAsync(channel));

        if (e.Exception == null || e.Initiator == ShutdownInitiator.Application)
            return;

        Logger.LogError(e.Exception,
            "Unexpected channel shutdown for Rabbit MQ. Wolverine will attempt to restart...");
    }

    private async Task ShutdownChannelAsync(IChannel oldChannel)
    {
        if (_disposed)
            return;

        await Locker.WaitAsync();
        try
        {
            if (_disposed)
                return;

            if (ReferenceEquals(_channel, oldChannel))
            {
                _channel = null;
                _state = AgentState.Disconnected;
            }
        }
        finally
        {
            Locker.Release();
        }

        //Close / dispose the old channel outside the Locker
        await DisposeChannelAsync(oldChannel);
    }

    private async ValueTask DisposeChannelAsync(IChannel? channel)
    {
        if (channel is null)
            return;

        try
        {
            channel.CallbackExceptionAsync -= HandleChannelExceptionAsync;
            channel.ChannelShutdownAsync -= HandleChannelShutdownAsync;
            await channel.CloseAsync();
            channel.Dispose();
        }
        catch (Exception e) when (e is ObjectDisposedException or AlreadyClosedException)
        {
            // The channel is dead - nothing to dispose
        }
    }

    internal virtual Task ReconnectedAsync()
    {
        return Task.CompletedTask;
    }

    private async Task TeardownChannelAsync()
    {
        await DisposeChannelAsync(_channel);
        _channel = null;
        _state = AgentState.Disconnected;
    }

    /// <summary>
    /// Tears down the current channel, creates a new one, and runs a setup action,
    /// all under the Locker. Used by subclasses during reconnection.
    /// </summary>
    protected async Task ReplaceChannelAndSetupAsync(Func<IChannel, ValueTask> setupAction)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await Locker.WaitAsync();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            await TeardownChannelAsync();
            await StartNewChannelAsync();
            await setupAction(_channel!);
        }
        finally
        {
            Locker.Release();
        }
    }
}

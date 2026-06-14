using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Resources;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Wolverine;
using Wolverine.Logging;
using Wolverine.Runtime;
using Wolverine.Transports;
using Xunit.Abstractions;

namespace CircuitBreakingTests;

[Collection("circuit_breaker")]
public abstract class CircuitBreakerIntegrationContext(ITestOutputHelper output)
    : IAsyncLifetime, IObserver<IWolverineEvent>
{
    private readonly List<ListenerState> _recordedStates = [];
    private readonly List<Task> _tasks = [];
    private IHost _host = null!;
    private Recorder _recorder = null!;
    private WolverineRuntime _runtime = null!;
    private IDisposable _trackerSubscription = null!;
    protected string _queueName = null!;

    public async Task InitializeAsync(){
        _queueName = $"{GetType().Name}_{DateTime.UtcNow:yyyyMMddHHmmss}";
        _host = await Host.CreateDefaultBuilder()
            .UseWolverine(configureListener)
            .UseResourceSetupOnStartup(StartupAction.ResetState)
            .ConfigureServices(services =>
            {
                services.AddSingleton(output);
                services.AddSingleton(typeof(ILogger<>), typeof(OutputLogger<>));
                services.AddSingleton<Recorder>();
            })
            .StartAsync();

        _recorder = _host.Services.GetRequiredService<Recorder>();
        _runtime = _host.Services.GetRequiredService<IWolverineRuntime>().As<WolverineRuntime>();
        _trackerSubscription = _runtime.Tracker.Subscribe(this);
    }

    public async Task DisposeAsync()
    {
        _trackerSubscription.Dispose();
        await _host.TeardownResources();
        await _host.StopAsync();
        _host.Dispose();
    }

    void IObserver<IWolverineEvent>.OnCompleted()
    {
    }

    void IObserver<IWolverineEvent>.OnError(Exception error)
    {
    }

    void IObserver<IWolverineEvent>.OnNext(IWolverineEvent value)
    {
        if (value is ListenerState state)
        {
            output.WriteLine($"CB status: {state.Status} " +
                $"(unique: {_recorder.GetProcessedCount()})");
            _recordedStates.Add(state);
        }
    }

    protected abstract void configureListener(WolverineOptions opts);

    protected void assertTheCircuitBreakerNeverTripped()
    {
        _recordedStates.Any(x => x.Status == ListeningStatus.Stopped).ShouldBeFalse();
    }

    protected void assertTheCircuitBreakerTripped()
    {
        _recordedStates.Any(x => x.Status == ListeningStatus.Stopped).ShouldBeTrue();
    }

    protected void assertTheCircuitBreakerWasReset()
    {
        assertTheCircuitBreakerTripped();

        _recordedStates.Last().Status.ShouldBe(ListeningStatus.Accepting);
    }

    protected SometimesFails[] buildHundredMessages(int failurePercent)
    {
        var everyOther = Math.Floor((double)(100 / failurePercent));

        var messages = new SometimesFails[100];

        for (var i = 0; i < messages.Length; i++)
        {
            var shouldFail = i > 0 && i % everyOther == 0;

            var message = new SometimesFails(Guid.NewGuid(), i, shouldFail ? MessageResult.BadImage : MessageResult.Success,
                MessageResult.Success, MessageResult.Success);

            messages[i] = message;
        }

        return messages;
    }

    protected void publishHundredMessagesNow(int failures)
    {
        var messages = buildHundredMessages(failures);
        var publisher = _host.MessageBus();
        var task = Task.Run(async () =>
        {
            foreach (var message in messages)
            {
                await publisher.PublishAsync(message);
                _recorder.TrackPublished(message.Id);
            }

            output.WriteLine($"Finished publishing a batch with {failures}% failures");
        });

        _tasks.Add(task);
    }

    protected void delayPublishHundredMessages(TimeSpan delay, int failures)
    {
        var messages = buildHundredMessages(failures);
        var publisher = _host.MessageBus();
        var task = Task.Run(async () =>
        {
            await Task.Delay(delay);
            output.WriteLine($"Starting to publish a batch with {failures}% failures");

            foreach (var message in messages)
            {
                await publisher.PublishAsync(message);
                _recorder.TrackPublished(message.Id);
            }

            output.WriteLine($"Finished publishing a batch with {failures}% failures");
        });

        _tasks.Add(task);
    }

    protected Task afterAllMessagesArePublished()
    {
        return Task.WhenAll(_tasks);
    }

    [Fact]
    public async Task everything_is_wonderful_even_though_there_are_some_failures_so_do_not_ever_trip()
    {
        var messageWaiter = _recorder.WaitForMessagesToBeProcessed(output, 1200, 2.Minutes());

        publishHundredMessagesNow(5);
        publishHundredMessagesNow(5);
        publishHundredMessagesNow(5);
        publishHundredMessagesNow(5);
        delayPublishHundredMessages(5.Seconds(), 5);
        delayPublishHundredMessages(5.Seconds(), 5);
        delayPublishHundredMessages(5.Seconds(), 5);
        delayPublishHundredMessages(5.Seconds(), 5);
        delayPublishHundredMessages(10.Seconds(), 5);
        delayPublishHundredMessages(10.Seconds(), 5);
        delayPublishHundredMessages(10.Seconds(), 5);
        delayPublishHundredMessages(10.Seconds(), 5);

        await afterAllMessagesArePublished();

        await messageWaiter;

        assertTheCircuitBreakerNeverTripped();
    }

    [Fact]
    public async Task the_circuit_breaker_should_trip_and_restart()
    {
        var messageWaiter = _recorder.WaitForMessagesToBeProcessed(output, 1200, 2.Minutes());

        publishHundredMessagesNow(10);
        publishHundredMessagesNow(80);
        publishHundredMessagesNow(10);
        publishHundredMessagesNow(25);
        publishHundredMessagesNow(25);

        _ = Task.Run(async () =>
        {
            await Task.Delay(10.Seconds());
            _recorder.NeverFail = true;
        });

        delayPublishHundredMessages(5.Seconds(), 5);
        delayPublishHundredMessages(10.Seconds(), 5);
        delayPublishHundredMessages(10.Seconds(), 10);
        delayPublishHundredMessages(10.Seconds(), 5);
        delayPublishHundredMessages(15.Seconds(), 10);
        delayPublishHundredMessages(15.Seconds(), 10);
        delayPublishHundredMessages(15.Seconds(), 10);

        await afterAllMessagesArePublished();

        await messageWaiter;

        assertTheCircuitBreakerTripped();
        assertTheCircuitBreakerWasReset();
    }
}

public enum MessageResult
{
    Success,
    DivideByZero,
    BadImage
}

public record SometimesFails(Guid Id, int Number, MessageResult First, MessageResult Second, MessageResult Third);

public class OutputLogger<T>(ITestOutputHelper output) : ILogger<T>, IDisposable
{
    public void Dispose()
    {
    }

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        output.WriteLine(formatter(state, exception));
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return true;
    }

    public IDisposable BeginScope<TState>(TState state) where TState : notnull
    {
        return this;
    }
}
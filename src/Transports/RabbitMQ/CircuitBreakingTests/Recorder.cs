using Xunit.Abstractions;

namespace CircuitBreakingTests;

public class Recorder
{
    public int Received;
    private TaskCompletionSource<int> _completion = null!;
    private int _expected;
    private ITestOutputHelper _output = null!;
    public bool NeverFail { get; set; }

    public Task WaitForMessagesToBeProcessed(ITestOutputHelper output, int number, TimeSpan timeout)
    {
        NeverFail = false;
        _output = output;
        Received = 0;
        _completion = new TaskCompletionSource<int>();
        _expected = number;

        var timeout1 = new CancellationTokenSource(timeout);
        timeout1.Token.Register(() =>
        {
            _completion.TrySetException(new TimeoutException(
                $"Listener did not process the expected message count {number} in the time allowed. The actual was {Received}"));
        });

        return _completion.Task;
    }

    public void Increment()
    {
        Interlocked.Increment(ref Received);

        if (Received % 50 == 0)
        {
            _output.WriteLine("Received " + Received);
        }

        if (Received >= _expected)
        {
            _completion.TrySetResult(Received);
        }
    }
}
using System.Collections.Concurrent;
using Xunit.Abstractions;

namespace CircuitBreakingTests;

public class Recorder
{
    private TaskCompletionSource<int> _completion = null!;
    private readonly ConcurrentDictionary<Guid, int> _processedIds = new();
    private readonly ConcurrentBag<Guid> _publishedIds = [];
    private int _expected;
    private ITestOutputHelper _output = null!;
    
    public bool NeverFail { get; set; }

    public int GetProcessedCount() => _processedIds.Count;

    public void TrackPublished(Guid id)
    {
        _publishedIds.Add(id);
    }

    public Task WaitForMessagesToBeProcessed(ITestOutputHelper output, int number, TimeSpan timeout)
    {
        NeverFail = false;
        _output = output;
        _processedIds.Clear();
        _publishedIds.Clear();
        _completion = new TaskCompletionSource<int>();
        _expected = number;

        var timeoutCts = new CancellationTokenSource(timeout);
        timeoutCts.Token.Register(() =>
        {
            int uniqueCount = 0;
            int publishedCount = 0;
            var missing = new List<Guid>();
            try
            {
                uniqueCount = _processedIds.Count;
                publishedCount = _publishedIds.Count;
                missing = [.. _publishedIds.Except(_processedIds.Keys)];
                var sample = string.Join(", ", missing.Take(10).Select(x => x.ToString()[..8]));
                _output.WriteLine($"DIAG: Processed {uniqueCount}/{publishedCount} published messages");
                _output.WriteLine($"DIAG: {missing.Count} never processed. Sample: {sample}");
            }
            catch
            {
                // Test may have already completed — output helper may be disposed
            }

            _completion.TrySetException(new TimeoutException(
                $"Listener did not process the expected message count {number} in the time allowed. " +
                $"Only {uniqueCount} unique messages received. " +
                $"{missing.Count} of {publishedCount} published never made it."));
        });

        return _completion.Task;
    }

    public void Increment(Guid messageId, int attempt = 0)
    {
        _processedIds.AddOrUpdate(messageId, 1, (_, existing) => existing + 1);

        var uniqueCount = _processedIds.Count;

        var shortId = messageId.ToString()[..8];
        _output.WriteLine($"msg#{shortId} completed " +
                $"(attempt {attempt}, unique: {uniqueCount})");

        if (uniqueCount >= _expected)
            _completion.TrySetResult(uniqueCount);
    }
}
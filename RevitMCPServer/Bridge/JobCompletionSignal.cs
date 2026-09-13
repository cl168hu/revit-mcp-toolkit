#nullable enable
namespace RevitMcpServer.Bridge;

/// <summary>
/// Process-local broadcast raised after a durable job enters a terminal state.
/// SQLite remains the source of truth. The signal only lets HTTP long-poll
/// callers avoid repeatedly querying the database while a job is in progress.
/// </summary>
public sealed class JobCompletionSignal
{
    private readonly object _gate = new();
    private TaskCompletionSource<bool> _next = CreateSource();
    private long _version;

    public long CaptureVersion()
    {
        lock (_gate) return _version;
    }

    public void Pulse()
    {
        TaskCompletionSource<bool> current;
        lock (_gate)
        {
            _version++;
            current = _next;
            _next = CreateSource();
        }
        current.TrySetResult(true);
    }

    public async Task WaitForChangeAsync(
        long observedVersion,
        int timeoutMs,
        CancellationToken cancellationToken)
    {
        if (timeoutMs <= 0) return;

        Task signalTask;
        lock (_gate)
        {
            if (_version != observedVersion) return;
            signalTask = _next.Task;
        }

        using var delayCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var delayTask = Task.Delay(timeoutMs, delayCancellation.Token);
        var completed = await Task.WhenAny(signalTask, delayTask).ConfigureAwait(false);
        if (completed == signalTask)
        {
            delayCancellation.Cancel();
            cancellationToken.ThrowIfCancellationRequested();
            return;
        }
        if (completed == delayTask)
            cancellationToken.ThrowIfCancellationRequested();
    }

    private static TaskCompletionSource<bool> CreateSource()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

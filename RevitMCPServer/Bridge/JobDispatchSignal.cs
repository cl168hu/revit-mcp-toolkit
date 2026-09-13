#nullable enable
namespace RevitMcpServer.Bridge;

/// <summary>
/// Process-local broadcast used to wake every bridge dispatcher after a job is
/// persisted. SQLite remains the source of truth; this only avoids periodic
/// polling while the server process is alive.
/// </summary>
public sealed class JobDispatchSignal
{
    private readonly object _gate = new();
    private TaskCompletionSource<bool> _next = CreateSource();
    private long _version;

    /// <summary>
    /// Captures the current notification generation before a caller checks
    /// SQLite. If enqueue happens between that check and the subsequent wait,
    /// WaitForChangeAsync observes the generation change and returns immediately.
    /// </summary>
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

        var delayTask = Task.Delay(timeoutMs, cancellationToken);
        var completed = await Task.WhenAny(signalTask, delayTask).ConfigureAwait(false);
        if (completed == delayTask)
            cancellationToken.ThrowIfCancellationRequested();
    }

    private static TaskCompletionSource<bool> CreateSource()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

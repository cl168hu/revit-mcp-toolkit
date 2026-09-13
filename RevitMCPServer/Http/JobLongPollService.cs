#nullable enable
using System.Diagnostics;
using System.Globalization;
using RevitMcpServer.Bridge;
using RevitMcpServer.Engine;

namespace RevitMcpServer.Http;

/// <summary>
/// Bounded, cancellation-aware wait for a durable job to become terminal.
/// No Revit execution is performed here and the response row always comes
/// from SQLite after a completion signal or timeout.
/// </summary>
public sealed class JobLongPollService
{
    public const int MaxWaitMs = 30_000;

    private static readonly HashSet<string> TerminalStates = new(StringComparer.OrdinalIgnoreCase)
    {
        "SUCCEEDED", "FAILED", "TIMEOUT", "DEAD"
    };

    private readonly DurableQueue _durable;
    private readonly JobCompletionSignal _completionSignal;

    public JobLongPollService(DurableQueue durable, JobCompletionSignal completionSignal)
    {
        _durable = durable ?? throw new ArgumentNullException(nameof(durable));
        _completionSignal = completionSignal ?? throw new ArgumentNullException(nameof(completionSignal));
    }

    public static bool TryParseWaitMs(string? raw, out int waitMs, out string error)
    {
        waitMs = 0;
        error = string.Empty;
        if (string.IsNullOrWhiteSpace(raw)) return true;
        if (!int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out waitMs)
            || waitMs < 0
            || waitMs > MaxWaitMs)
        {
            error = $"waitMs must be an integer from 0 to {MaxWaitMs}.";
            return false;
        }
        return true;
    }

    public async Task<JobLongPollResult> WaitAsync(
        string jobId,
        int waitMs,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(jobId))
            return new JobLongPollResult(null, false, 0, string.Empty);
        if (waitMs < 0 || waitMs > MaxWaitMs)
            throw new ArgumentOutOfRangeException(nameof(waitMs));

        var watch = Stopwatch.StartNew();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var observedVersion = _completionSignal.CaptureVersion();
            object? row = await _durable.GetAsync(jobId).ConfigureAwait(false);
            if (row == null)
                return new JobLongPollResult(null, false, watch.ElapsedMilliseconds, string.Empty);

            var state = GetState(row);
            if (IsTerminal(state) || waitMs == 0)
                return new JobLongPollResult(row, false, watch.ElapsedMilliseconds, state);

            var remaining = waitMs - (int)Math.Min(int.MaxValue, watch.ElapsedMilliseconds);
            if (remaining <= 0)
                return new JobLongPollResult(row, true, watch.ElapsedMilliseconds, state);

            await _completionSignal.WaitForChangeAsync(
                observedVersion,
                remaining,
                cancellationToken).ConfigureAwait(false);
        }
    }

    public static bool IsTerminal(string? state)
        => !string.IsNullOrWhiteSpace(state) && TerminalStates.Contains(state);

    private static string GetState(object row)
    {
        if (row is IDictionary<string, object?> nullable
            && nullable.TryGetValue("state", out var value))
            return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

        if (row is System.Collections.IDictionary dictionary
            && dictionary.Contains("state"))
            return Convert.ToString(dictionary["state"], CultureInfo.InvariantCulture) ?? string.Empty;

        return string.Empty;
    }
}

public sealed record JobLongPollResult(
    object? Row,
    bool TimedOut,
    long WaitedMs,
    string State);

using System.Diagnostics;
using System.Collections.Generic;
using RevitMcpServer.Engine;

namespace RevitMcpServer.Bridge;

/// <summary>
/// Single claim point for Server-to-Add-in job dispatch. NP-0 keeps the legacy
/// 100 ms SQLite-backed long-poll behavior, but removes claim logic from the HTTP
/// endpoint so a named-pipe dispatcher can share the same serialization gate later.
/// </summary>
public sealed class BridgeDispatchCoordinator
{
    private readonly DurableQueue _durable;
    private readonly JobDispatchSignal _dispatchSignal;
    private readonly SemaphoreSlim _claimGate = new(1, 1);

    public BridgeDispatchCoordinator(DurableQueue durable, JobDispatchSignal dispatchSignal)
    {
        _durable = durable ?? throw new ArgumentNullException(nameof(durable));
        _dispatchSignal = dispatchSignal ?? throw new ArgumentNullException(nameof(dispatchSignal));
    }

    public async Task<BridgeDispatchItem?> WaitAndClaimAsync(int waitMs, CancellationToken cancellationToken)
        => await WaitAndClaimAsync(
            waitMs,
            cancellationToken,
            includeMethods: null,
            excludeMethods: null,
            markRunning: true).ConfigureAwait(false);

    public async Task<BridgeDispatchItem?> WaitAndClaimAsync(
        int waitMs,
        CancellationToken cancellationToken,
        IReadOnlyCollection<string>? includeMethods,
        IReadOnlyCollection<string>? excludeMethods,
        bool markRunning,
        Func<string, string, bool>? jobPredicate = null)
    {
        if (waitMs < 0) waitMs = 0;

        var stopwatch = Stopwatch.StartNew();
        while (true)
        {
            // Capture the generation before checking SQLite. This closes the
            // enqueue race between a null claim and registering the async wait.
            var observedSignalVersion = _dispatchSignal.CaptureVersion();
            dynamic? job = null;
            await _claimGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                job = await _durable.ClaimAsync(includeMethods, excludeMethods, jobPredicate).ConfigureAwait(false);
            }
            finally
            {
                _claimGate.Release();
            }

            if (job != null)
            {
                string jobId = (string)job["job_id"];
                string method = (string)job["method"];
                string paramsJson = (string)job["params_json"];
                string rpcId = job["rpc_id"] != null ? Convert.ToString(job["rpc_id"]) ?? "1" : "1";
                if (markRunning)
                    await _durable.StartRunningAsync(jobId).ConfigureAwait(false);
                stopwatch.Stop();

                return new BridgeDispatchItem
                {
                    JobId = jobId,
                    Method = method,
                    ParamsJson = paramsJson,
                    RpcId = rpcId,
                    TimeoutSec = ToInt32(job["timeout_sec"], 60),
                    ClaimWaitMs = stopwatch.ElapsedMilliseconds
                };
            }

            if (waitMs <= 0 || stopwatch.ElapsedMilliseconds >= waitMs)
                return null;

            var remaining = waitMs - (int)Math.Min(int.MaxValue, stopwatch.ElapsedMilliseconds);
            await _dispatchSignal.WaitForChangeAsync(
                observedSignalVersion,
                Math.Min(1000, Math.Max(1, remaining)),
                cancellationToken).ConfigureAwait(false);
        }
    }

    private static int ToInt32(object? value, int fallback)
    {
        try { return Convert.ToInt32(value); }
        catch { return fallback; }
    }
}

public sealed class BridgeDispatchItem
{
    public string JobId { get; set; } = string.Empty;
    public string Method { get; set; } = string.Empty;
    public string ParamsJson { get; set; } = "{}";
    public string RpcId { get; set; } = "1";
    public int TimeoutSec { get; set; } = 60;
    public long ClaimWaitMs { get; set; }
}

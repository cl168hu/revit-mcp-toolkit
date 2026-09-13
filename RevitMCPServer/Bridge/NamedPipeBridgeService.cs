#nullable enable
using System.Diagnostics;
using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using RevitMCP.Abstractions.Bridge;
using RevitMcpServer.Engine;
using RevitMcpServer.Infra;

namespace RevitMcpServer.Bridge;

/// <summary>
/// NP-1 handshake, NP-2.1 read-only commands, and the feature-gated NP-3B
/// explicit dry-run write channel with persisted receipt recovery.
/// Apply calls and legacy aliases remain on HTTP.
/// </summary>
public sealed class NamedPipeBridgeService : BackgroundService
{
    private static readonly IReadOnlyCollection<string> ReadOnlyMethods
        = NamedPipeReadOnlyAllowlist.CanonicalMethods;
    private static readonly IReadOnlyCollection<string> PipeCandidateMethods
        = ReadOnlyMethods.Concat(NamedPipeCommandPolicy.DryRunWriteMethods).ToArray();
    private readonly object _stateGate = new();
    private readonly ILogger<NamedPipeBridgeService> _logger;
    private readonly BridgeDispatchCoordinator _dispatchCoordinator;
    private readonly DurableQueue _durable;
    private readonly bool _enabled;
    private readonly bool _readOnlyDispatchEnabled;
    private readonly bool _writeDryRunDispatchEnabled;
    private readonly bool _writeReceiptRecoveryEnabled;
    private readonly TimeSpan _acceptanceTimeout;
    private readonly TimeSpan _commandHeartbeatInterval;
    private readonly TimeSpan _minimumCommandTimeout;
    private readonly int _port;
    private readonly string _nonce;
    private readonly string _pipeName;
    private string _status;
    private int _revitProcessId;
    private string _revitVersion = string.Empty;
    private DateTimeOffset? _connectedUtc;
    private DateTimeOffset? _lastSeenUtc;
    private long _connectionCount;
    private bool _bridgeTimingFractionalSecondsPreserved;
    private bool _writeRecoveryReady;
    private long _recoveredResultCount;
    private long _recoveredUnknownCount;
    private long _recoveryRejectedCount;
    private NamedPipeServerStream? _activePipe;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly ConcurrentDictionary<string, PendingPipeJob> _pending = new(StringComparer.Ordinal);

    public NamedPipeBridgeService(
        int port,
        bool enabled,
        bool readOnlyDispatchEnabled,
        BridgeDispatchCoordinator dispatchCoordinator,
        DurableQueue durable,
        ILogger<NamedPipeBridgeService> logger,
        bool writeDryRunDispatchEnabled = false,
        bool writeReceiptRecoveryEnabled = true,
        TimeSpan? acceptanceTimeout = null,
        TimeSpan? commandHeartbeatInterval = null,
        TimeSpan? minimumCommandTimeout = null)
    {
        _port = port;
        _enabled = enabled;
        _readOnlyDispatchEnabled = enabled && readOnlyDispatchEnabled;
        _writeDryRunDispatchEnabled = enabled && writeDryRunDispatchEnabled;
        _writeReceiptRecoveryEnabled = enabled && writeReceiptRecoveryEnabled;
        _dispatchCoordinator = dispatchCoordinator ?? throw new ArgumentNullException(nameof(dispatchCoordinator));
        _durable = durable ?? throw new ArgumentNullException(nameof(durable));
        _logger = logger;
        _acceptanceTimeout = RequirePositive(
            acceptanceTimeout ?? TimeSpan.FromSeconds(10),
            nameof(acceptanceTimeout));
        _commandHeartbeatInterval = RequirePositive(
            commandHeartbeatInterval ?? TimeSpan.FromSeconds(10),
            nameof(commandHeartbeatInterval));
        _minimumCommandTimeout = RequirePositive(
            minimumCommandTimeout ?? TimeSpan.FromMinutes(10),
            nameof(minimumCommandTimeout));
        _nonce = Guid.NewGuid().ToString("N");
        _pipeName = "RevitMCP.v1.s" + Process.GetCurrentProcess().Id + "." + _nonce.Substring(0, 16);
        _status = enabled ? "listening" : "disabled";
    }

    public object GetDescriptor()
    {
        lock (_stateGate)
        {
            var pipeConnected = string.Equals(_status, "connected", StringComparison.Ordinal)
                && _activePipe != null
                && _activePipe.IsConnected;
            var writeDryRunTransport = _writeDryRunDispatchEnabled && pipeConnected
                ? (_writeRecoveryReady ? "namedPipe" : "blockedReceiptRecovery")
                : "legacyHttp";
            return new
            {
                ok = true,
                enabled = _enabled,
                handshakeOnly = !_readOnlyDispatchEnabled && !_writeDryRunDispatchEnabled,
                readOnlyDispatchEnabled = _readOnlyDispatchEnabled,
                readOnlyMethods = ReadOnlyMethods,
                writeDryRunDispatchEnabled = _writeDryRunDispatchEnabled,
                writeDryRunMethods = NamedPipeCommandPolicy.DryRunWriteMethods,
                writeDryRunExplicitTrueRequired = true,
                writeApplyDispatchEnabled = false,
                unknownWriteOutcomePolicy = "dead-no-auto-retry",
                writeReceiptRecoveryEnabled = _writeReceiptRecoveryEnabled,
                writeReceiptRecoveryReady = _writeRecoveryReady,
                writeReceiptJournalMode = "append-only-jsonl-no-delete",
                recoveredResultCount = _recoveredResultCount,
                recoveredUnknownCount = _recoveredUnknownCount,
                recoveryRejectedCount = _recoveryRejectedCount,
                benchmarkLegacyAlias = "get_rooms",
                benchmarkLegacyAliases = NamedPipeReadOnlyAllowlist.LegacyAliasesByCanonical,
                bridgeTimingFractionalSecondsPreserved = _bridgeTimingFractionalSecondsPreserved,
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                pipeName = _enabled ? _pipeName : string.Empty,
                nonce = _enabled ? _nonce : string.Empty,
                serverProcessId = Process.GetCurrentProcess().Id,
                port = _port,
                status = _status,
                revitProcessId = _revitProcessId,
                revitVersion = _revitVersion,
                connectedUtc = _connectedUtc?.ToString("o"),
                lastSeenUtc = _lastSeenUtc?.ToString("o"),
                connectionCount = _connectionCount,
                pendingCount = _pending.Count,
                acceptanceTimeoutMs = (long)_acceptanceTimeout.TotalMilliseconds,
                commandHeartbeatIntervalMs = (long)_commandHeartbeatInterval.TotalMilliseconds,
                minimumCommandTimeoutMs = (long)_minimumCommandTimeout.TotalMilliseconds,
                commandTransport = "legacyHttp",
                readOnlyCommandTransport = _readOnlyDispatchEnabled && string.Equals(_status, "connected", StringComparison.Ordinal)
                    ? "namedPipe"
                    : "legacyHttp",
                writeDryRunCommandTransport = writeDryRunTransport,
                writeDryRunDispatchBlockedReason = string.Equals(writeDryRunTransport, "blockedReceiptRecovery", StringComparison.Ordinal)
                    ? "persistent receipt recovery is incomplete or rejected"
                    : string.Empty,
                durableQueue = true
            };
        }
    }

    public bool CanReserveReadOnlyJobs
    {
        get
        {
            lock (_stateGate)
            {
                return _readOnlyDispatchEnabled
                    && string.Equals(_status, "connected", StringComparison.Ordinal)
                    && _activePipe != null
                    && _activePipe.IsConnected;
            }
        }
    }

    public IReadOnlyCollection<string> GetReservedReadOnlyMethods()
        => ReadOnlyMethods;

    public bool CanReservePipeJobs
    {
        get
        {
            lock (_stateGate)
            {
                return (_readOnlyDispatchEnabled || _writeDryRunDispatchEnabled)
                    && string.Equals(_status, "connected", StringComparison.Ordinal)
                    && _activePipe != null
                    && _activePipe.IsConnected;
            }
        }
    }

    public bool ShouldReserveJobForPipe(string? method, string? paramsJson)
    {
        if (!CanReservePipeJobs) return false;
        return !string.IsNullOrEmpty(NamedPipeCommandPolicy.Classify(
            method,
            paramsJson,
            _readOnlyDispatchEnabled,
            _writeDryRunDispatchEnabled));
    }

    private bool ShouldDispatchJobOnPipe(string? method, string? paramsJson)
    {
        if (!CanReservePipeJobs) return false;
        var isReadOnly = _readOnlyDispatchEnabled && NamedPipeReadOnlyAllowlist.Contains(method);
        if (isReadOnly) return true;

        lock (_stateGate)
        {
            return _writeRecoveryReady
                && _writeDryRunDispatchEnabled
                && NamedPipeCommandPolicy.IsExplicitDryRunWrite(method, paramsJson);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_enabled)
        {
            _logger.LogInformation("Named-pipe bridge NP-1 is disabled; legacy HTTP remains active.");
            return;
        }

        _logger.LogInformation(
            "Named-pipe bridge NP-1 listening pipe={PipeName} protocol={Protocol}; command transport remains legacy HTTP.",
            _pipeName,
            NamedPipeFrameCodec.ProtocolVersion);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var pipe = CreateServerPipe();
                SetStatus("listening", clearClient: true);
                await pipe.WaitForConnectionAsync(stoppingToken).ConfigureAwait(false);
                await HandleConnectionAsync(pipe, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (EndOfStreamException)
            {
                // A Revit shutdown or add-in unload closes the client side while the
                // server is waiting for the next frame. This is an ordinary session
                // disconnect, not a bridge fault; return to the listening state.
                SetStatus("disconnected", clearClient: true);
                _logger.LogInformation("Named-pipe bridge client closed the session; returning to the listening state.");
            }
            catch (Exception ex)
            {
                SetStatus("faulted", clearClient: true);
                _logger.LogWarning(ex, "Named-pipe bridge NP-1 connection failed; legacy HTTP remains active.");
                try { await Task.Delay(1000, stoppingToken).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
        }

        SetStatus("stopped", clearClient: true);
    }

    private NamedPipeServerStream CreateServerPipe()
    {
        return new NamedPipeServerStream(
            _pipeName,
            PipeDirection.InOut,
            1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly,
            64 * 1024,
            64 * 1024);
    }

    private async Task HandleConnectionAsync(NamedPipeServerStream pipe, CancellationToken stoppingToken)
    {
        var helloText = await ReadWithTimeoutAsync(pipe, TimeSpan.FromSeconds(10), stoppingToken).ConfigureAwait(false);
        using var helloDocument = JsonDocument.Parse(helloText);
        var hello = helloDocument.RootElement;
        var kind = GetString(hello, "kind");
        var protocol = GetString(hello, "protocol");
        var nonce = GetString(hello, "nonce");
        var requestId = GetString(hello, "requestId");
        var revitPid = GetInt32(hello, "revitProcessId");
        var revitVersion = GetString(hello, "revitVersion");
        var bridgeTimingFractionalSecondsPreserved = GetBoolean(hello, "bridgeTimingFractionalSecondsPreserved");
        var clientReceiptRecoverySupported = GetBoolean(hello, "writeReceiptRecoverySupported");
        var declaredUnresolvedReceiptCount = Math.Max(0, GetInt32(hello, "unresolvedWriteReceiptCount"));

        if (!string.Equals(kind, "hello", StringComparison.Ordinal)
            || !string.Equals(protocol, NamedPipeFrameCodec.ProtocolVersion, StringComparison.Ordinal)
            || !string.Equals(nonce, _nonce, StringComparison.Ordinal)
            || revitPid <= 0)
        {
            var rejected = JsonSerializer.Serialize(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "helloAck",
                requestId,
                ok = false,
                code = "PIPE_HANDSHAKE_REJECTED",
                msg = "Protocol, nonce, kind, or Revit process ID was invalid."
            });
            await NamedPipeFrameCodec.WriteJsonAsync(pipe, rejected, stoppingToken).ConfigureAwait(false);
            throw new InvalidDataException("Named-pipe handshake rejected.");
        }

        lock (_stateGate)
        {
            _status = "connected";
            _activePipe = pipe;
            _revitProcessId = revitPid;
            _revitVersion = revitVersion;
            _bridgeTimingFractionalSecondsPreserved = bridgeTimingFractionalSecondsPreserved;
            _connectedUtc = DateTimeOffset.UtcNow;
            _lastSeenUtc = _connectedUtc;
            _connectionCount++;
            _writeRecoveryReady = false;
        }

        var receiptRecoveryActive = _writeReceiptRecoveryEnabled && clientReceiptRecoverySupported;
        var effectiveWriteDryRunDispatch = _writeDryRunDispatchEnabled && receiptRecoveryActive;

        var accepted = JsonSerializer.Serialize(new
        {
            protocol = NamedPipeFrameCodec.ProtocolVersion,
            kind = "helloAck",
            requestId,
            ok = true,
            handshakeOnly = !_readOnlyDispatchEnabled && !effectiveWriteDryRunDispatch,
            readOnlyDispatchEnabled = _readOnlyDispatchEnabled,
            readOnlyMethods = ReadOnlyMethods,
            writeDryRunDispatchEnabled = effectiveWriteDryRunDispatch,
            writeDryRunMethods = NamedPipeCommandPolicy.DryRunWriteMethods,
            writeDryRunExplicitTrueRequired = true,
            writeApplyDispatchEnabled = false,
            writeReceiptRecoveryEnabled = receiptRecoveryActive,
            writeReceiptRecoveryRequiredForDryRun = true,
            declaredUnresolvedReceiptCount,
            bridgeTimingFractionalSecondsPreserved,
            commandTransport = "legacyHttp",
            readOnlyCommandTransport = _readOnlyDispatchEnabled ? "namedPipe" : "legacyHttp",
            serverProcessId = Process.GetCurrentProcess().Id,
            port = _port,
            connectedUtc = DateTimeOffset.UtcNow.ToString("o")
        });
        await NamedPipeFrameCodec.WriteJsonAsync(pipe, accepted, stoppingToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Named-pipe bridge connected revitPid={RevitPid} revitVersion={RevitVersion} readOnlyDispatch={ReadOnlyDispatch} writeDryRunDispatch={WriteDryRunDispatch} receiptRecovery={ReceiptRecovery} unresolvedReceipts={UnresolvedReceipts}; apply transport remains legacy HTTP.",
            revitPid,
            revitVersion,
            _readOnlyDispatchEnabled,
            effectiveWriteDryRunDispatch,
            receiptRecoveryActive,
            declaredUnresolvedReceiptCount);

        using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var recoverySession = new ReceiptRecoverySession(receiptRecoveryActive, declaredUnresolvedReceiptCount);
        Task? readTask = null;
        Task? dispatchTask = null;
        try
        {
            readTask = ReadLoopAsync(pipe, recoverySession, sessionCts.Token);
            if (receiptRecoveryActive)
            {
                await WaitWithTimeoutAsync(
                    recoverySession.Completed.Task,
                    TimeSpan.FromSeconds(15),
                    sessionCts.Token,
                    "Persistent write-receipt recovery handshake timed out.").ConfigureAwait(false);
                lock (_stateGate) _writeRecoveryReady = !recoverySession.HasRejected;
                if (recoverySession.HasRejected)
                {
                    _logger.LogWarning(
                        "Persistent write-receipt recovery completed with {RejectedCount} rejected receipt(s); dry-run write dispatch remains disabled for this connection.",
                        recoverySession.RejectedCount);
                }
            }

            dispatchTask = (_readOnlyDispatchEnabled || effectiveWriteDryRunDispatch)
                ? DispatchPipeLoopAsync(pipe, sessionCts.Token)
                : Task.Delay(Timeout.Infinite, sessionCts.Token);

            var completed = await Task.WhenAny(readTask, dispatchTask).ConfigureAwait(false);
            await completed.ConfigureAwait(false);
        }
        finally
        {
            try { sessionCts.Cancel(); } catch { }
            try
            {
                var tasks = new[] { readTask, dispatchTask }.Where(x => x != null).Cast<Task>().ToArray();
                if (tasks.Length > 0) await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch { /* the primary connection exception is reported by ExecuteAsync */ }
            CancelPending(new IOException("Named-pipe session disconnected."));
            SetStatus("disconnected", clearClient: true);
            _logger.LogInformation("Named-pipe bridge disconnected; read jobs may fall back to HTTP, while an in-flight dry-run write is stopped for recovery review without automatic replay.");
        }
    }

    private async Task ReadLoopAsync(
        NamedPipeServerStream pipe,
        ReceiptRecoverySession recoverySession,
        CancellationToken cancellationToken)
    {
        while (pipe.IsConnected && !cancellationToken.IsCancellationRequested)
        {
            var text = await NamedPipeFrameCodec.ReadJsonAsync(pipe, cancellationToken).ConfigureAwait(false);
            using var document = JsonDocument.Parse(text);
            var root = document.RootElement;
            var messageKind = GetString(root, "kind");
            var messageRequestId = GetString(root, "requestId");
            if (!string.Equals(GetString(root, "protocol"), NamedPipeFrameCodec.ProtocolVersion, StringComparison.Ordinal))
                throw new InvalidDataException("Named-pipe message protocol mismatch.");

            lock (_stateGate) _lastSeenUtc = DateTimeOffset.UtcNow;

            if (string.Equals(messageKind, "ping", StringComparison.Ordinal))
            {
                await WriteFrameAsync(pipe, JsonSerializer.Serialize(new
                {
                    protocol = NamedPipeFrameCodec.ProtocolVersion,
                    kind = "pong",
                    requestId = messageRequestId,
                    ok = true,
                    serverUtc = DateTimeOffset.UtcNow.ToString("o")
                }), cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (string.Equals(messageKind, "recoveryBegin", StringComparison.Ordinal))
            {
                if (!recoverySession.Enabled)
                    throw new InvalidDataException("Receipt recovery was not negotiated for this session.");
                recoverySession.Begin(
                    GetString(root, "batchId"),
                    Math.Max(0, GetInt32(root, "receiptCount")));
                continue;
            }

            if (string.Equals(messageKind, "recoveryResponse", StringComparison.Ordinal))
            {
                await HandleRecoveredResponseAsync(pipe, root, recoverySession, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (string.Equals(messageKind, "recoveryUnknown", StringComparison.Ordinal))
            {
                await HandleRecoveredUnknownAsync(pipe, root, recoverySession, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (string.Equals(messageKind, "recoveryEnd", StringComparison.Ordinal))
            {
                var sentCount = Math.Max(0, GetInt32(root, "sentCount"));
                recoverySession.End(GetString(root, "batchId"), sentCount);
                await WriteFrameAsync(pipe, JsonSerializer.Serialize(new
                {
                    protocol = NamedPipeFrameCodec.ProtocolVersion,
                    kind = "recoveryBatchAck",
                    requestId = messageRequestId,
                    batchId = GetString(root, "batchId"),
                    ok = !recoverySession.HasRejected,
                    code = recoverySession.HasRejected ? "PIPE_RECEIPT_RECOVERY_REJECTED" : "OK",
                    msg = recoverySession.HasRejected
                        ? "One or more persisted receipts were rejected. Dry-run write dispatch remains disabled."
                        : "Persistent receipt recovery completed.",
                    processedCount = recoverySession.ProcessedCount,
                    rejectedCount = recoverySession.RejectedCount,
                    serverUtc = DateTimeOffset.UtcNow.ToString("o")
                }), cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (!_pending.TryGetValue(messageRequestId, out var pending))
            {
                _logger.LogDebug("Ignoring late named-pipe message kind={Kind} requestId={RequestId}.", messageKind, messageRequestId);
                continue;
            }

            if (string.Equals(messageKind, "accepted", StringComparison.Ordinal))
            {
                pending.AcceptedUtc = DateTimeOffset.UtcNow;
                pending.Accepted.TrySetResult(true);
                continue;
            }

            if (string.Equals(messageKind, "started", StringComparison.Ordinal))
            {
                pending.StartedUtc = DateTimeOffset.UtcNow;
                pending.Accepted.TrySetResult(true);
                continue;
            }

            if (string.Equals(messageKind, "response", StringComparison.Ordinal))
            {
                var ok = root.TryGetProperty("ok", out var okElement) && okElement.ValueKind == JsonValueKind.True;
                var resultJson = GetString(root, "resultJson");
                var code = GetString(root, "code");
                var msg = GetString(root, "msg");
                pending.Response.TrySetResult(new PipeJobResponse(ok, resultJson, code, msg));
                continue;
            }

            if (string.Equals(messageKind, "error", StringComparison.Ordinal))
            {
                pending.Response.TrySetException(new InvalidOperationException(
                    GetString(root, "code") + ": " + GetString(root, "msg")));
                continue;
            }

            await WriteFrameAsync(pipe, JsonSerializer.Serialize(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "error",
                requestId = messageRequestId,
                ok = false,
                code = "PIPE_MESSAGE_KIND_UNSUPPORTED",
                msg = "Unsupported named-pipe message kind."
            }), cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HandleRecoveredResponseAsync(
        NamedPipeServerStream pipe,
        JsonElement root,
        ReceiptRecoverySession recoverySession,
        CancellationToken cancellationToken)
    {
        if (!recoverySession.Enabled || !recoverySession.Begun)
            throw new InvalidDataException("Receipt recovery response arrived before recoveryBegin.");

        var requestId = GetString(root, "requestId");
        var jobId = GetString(root, "jobId");
        var method = GetString(root, "method");
        var paramsSha256 = GetString(root, "paramsSha256");
        var resultSha256 = GetString(root, "resultSha256");
        var resultJson = GetString(root, "resultJson");
        PipeReceiptRecoveryResult result;
        if (Encoding.UTF8.GetByteCount(resultJson) > NamedPipeWriteReceiptJournal.MaximumPersistedResultBytes)
        {
            result = PipeReceiptRecoveryResult.Reject(
                "RECOVERY_RESULT_TOO_LARGE",
                "Recovered result exceeds the 16 MiB persistence limit.");
        }
        else
        {
            result = await _durable.ReconcileRecoveredResultAsync(
                jobId,
                method,
                paramsSha256,
                resultJson,
                resultSha256).ConfigureAwait(false);
        }

        recoverySession.RecordProcessed(result.Ok);
        if (result.Ok) Interlocked.Increment(ref _recoveredResultCount);
        else Interlocked.Increment(ref _recoveryRejectedCount);
        Logging.Append($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] PIPE RECEIPT RECOVERY: kind=completed jobId={jobId} method={method} ok={result.Ok} code={result.Code} reexecute=false");

        await WriteRecoveryAckAsync(pipe, requestId, jobId, result, cancellationToken).ConfigureAwait(false);
    }

    private async Task HandleRecoveredUnknownAsync(
        NamedPipeServerStream pipe,
        JsonElement root,
        ReceiptRecoverySession recoverySession,
        CancellationToken cancellationToken)
    {
        if (!recoverySession.Enabled || !recoverySession.Begun)
            throw new InvalidDataException("Unknown receipt recovery arrived before recoveryBegin.");

        var requestId = GetString(root, "requestId");
        var jobId = GetString(root, "jobId");
        var method = GetString(root, "method");
        var paramsSha256 = GetString(root, "paramsSha256");
        var receiptState = GetString(root, "receiptState");
        var result = await _durable.ConfirmRecoveredUnknownAsync(
            jobId,
            method,
            paramsSha256,
            receiptState).ConfigureAwait(false);

        recoverySession.RecordProcessed(result.Ok);
        if (result.Ok) Interlocked.Increment(ref _recoveredUnknownCount);
        else Interlocked.Increment(ref _recoveryRejectedCount);
        Logging.Append($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] PIPE RECEIPT RECOVERY: kind=unknown jobId={jobId} method={method} receiptState={receiptState} ok={result.Ok} code={result.Code} reexecute=false");

        await WriteRecoveryAckAsync(pipe, requestId, jobId, result, cancellationToken).ConfigureAwait(false);
    }

    private Task WriteRecoveryAckAsync(
        NamedPipeServerStream pipe,
        string requestId,
        string jobId,
        PipeReceiptRecoveryResult result,
        CancellationToken cancellationToken)
    {
        return WriteFrameAsync(pipe, JsonSerializer.Serialize(new
        {
            protocol = NamedPipeFrameCodec.ProtocolVersion,
            kind = "recoveryAck",
            requestId,
            jobId,
            ok = result.Ok,
            code = result.Code,
            msg = result.Message,
            state = result.State,
            reexecuted = false,
            serverUtc = DateTimeOffset.UtcNow.ToString("o")
        }), cancellationToken);
    }

    private async Task DispatchPipeLoopAsync(NamedPipeServerStream pipe, CancellationToken cancellationToken)
    {
        while (pipe.IsConnected && !cancellationToken.IsCancellationRequested)
        {
            var job = await _dispatchCoordinator.WaitAndClaimAsync(
                30_000,
                cancellationToken,
                includeMethods: PipeCandidateMethods,
                excludeMethods: null,
                markRunning: false,
                jobPredicate: (method, paramsJson) => ShouldDispatchJobOnPipe(method, paramsJson)).ConfigureAwait(false);
            if (job == null) continue;

            await DispatchOnePipeJobAsync(pipe, job, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task DispatchOnePipeJobAsync(
        NamedPipeServerStream pipe,
        BridgeDispatchItem job,
        CancellationToken cancellationToken)
    {
        var requestId = job.JobId;
        var pending = new PendingPipeJob(job.JobId, job.RpcId, job.Method);
        if (!_pending.TryAdd(requestId, pending))
            throw new InvalidOperationException("A named-pipe request with the same job ID is already pending.");

        var dispatchWatch = Stopwatch.StartNew();
        var completedInDatabase = false;
        var requestSent = false;
        var commandClass = NamedPipeCommandPolicy.Classify(
            job.Method,
            job.ParamsJson,
            _readOnlyDispatchEnabled,
            _writeDryRunDispatchEnabled);
        if (string.IsNullOrEmpty(commandClass))
        {
            _pending.TryRemove(requestId, out _);
            await _durable.RequeueAsync(job.JobId).ConfigureAwait(false);
            return;
        }
        var isDryRunWrite = string.Equals(
            commandClass,
            NamedPipeCommandPolicy.DryRunWriteClass,
            StringComparison.Ordinal);
        var transportName = isDryRunWrite ? "namedPipeDryRun" : "namedPipeReadOnly";
        try
        {
            JsonElement parameters;
            try { parameters = JsonSerializer.Deserialize<JsonElement>(job.ParamsJson); }
            catch { parameters = JsonSerializer.Deserialize<JsonElement>("{}"); }
            var paramsSha256 = NamedPipeReceiptHash.ComputeCanonicalJsonSha256(job.ParamsJson);

            var request = JsonSerializer.Serialize(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "request",
                requestId,
                jobId = job.JobId,
                rpcId = job.RpcId,
                method = job.Method,
                @params = parameters,
                paramsSha256,
                timeoutMs = Math.Max(10_000, job.TimeoutSec * 1000),
                serverSentUtc = DateTimeOffset.UtcNow.ToString("o"),
                transport = transportName,
                commandClass
            });

            var sendWatch = Stopwatch.StartNew();
            await WriteFrameAsync(pipe, request, cancellationToken).ConfigureAwait(false);
            requestSent = true;
            sendWatch.Stop();

            await WaitWithTimeoutAsync(pending.Accepted.Task, _acceptanceTimeout, cancellationToken).ConfigureAwait(false);
            await _durable.StartRunningAsync(job.JobId).ConfigureAwait(false);

            // A modal Revit dialog can legitimately delay ExternalEvent execution.
            // Keep the command alive for at least ten minutes and refresh its
            // durable heartbeat while waiting.
            var requestedCommandTimeout = TimeSpan.FromSeconds(Math.Max(0, job.TimeoutSec));
            var effectiveCommandTimeout = requestedCommandTimeout > _minimumCommandTimeout
                ? requestedCommandTimeout
                : _minimumCommandTimeout;
            var commandDeadline = DateTimeOffset.UtcNow.Add(effectiveCommandTimeout);
            PipeJobResponse response;
            while (true)
            {
                var completed = await Task.WhenAny(
                    pending.Response.Task,
                    Task.Delay(_commandHeartbeatInterval, cancellationToken)).ConfigureAwait(false);
                if (completed == pending.Response.Task)
                {
                    response = await pending.Response.Task.ConfigureAwait(false);
                    break;
                }

                cancellationToken.ThrowIfCancellationRequested();
                if (DateTimeOffset.UtcNow >= commandDeadline)
                    throw new TimeoutException("Named-pipe command timed out while waiting for Revit ExternalEvent.");
                await _durable.HeartbeatAsync(job.JobId).ConfigureAwait(false);
            }

            if (!response.Ok || string.IsNullOrWhiteSpace(response.ResultJson))
                throw new InvalidOperationException((response.Code ?? "PIPE_COMMAND_FAILED") + ": " + (response.Message ?? "No result JSON was returned."));

            await _durable.CompleteAsync(job.JobId, response.ResultJson).ConfigureAwait(false);
            completedInDatabase = true;
            dispatchWatch.Stop();

            await WriteFrameAsync(pipe, JsonSerializer.Serialize(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "ack",
                requestId,
                jobId = job.JobId,
                ok = true,
                serverCompletedUtc = DateTimeOffset.UtcNow.ToString("o")
            }), cancellationToken).ConfigureAwait(false);

            Logging.Append($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] PIPE COMPLETE: transport={transportName} method={job.Method} jobId={job.JobId} claimWaitMs={job.ClaimWaitMs} pipeSendMs={sendWatch.ElapsedMilliseconds} acceptedMs={ElapsedBetween(pending.CreatedUtc, pending.AcceptedUtc)} totalMs={dispatchWatch.ElapsedMilliseconds}");
        }
        catch (Exception ex)
        {
            if (!completedInDatabase)
            {
                if (isDryRunWrite && requestSent)
                {
                    // Even a dry-run uses a write-class command handler. Once the
                    // frame has left the Server, never infer that it was not run.
                    // Mark the job terminal for review instead of replaying it.
                    try
                    {
                        await _durable.DeadAsync(
                            job.JobId,
                            "PIPE_DRY_RUN_OUTCOME_UNKNOWN",
                            "The named-pipe connection ended after dispatch. The command was not automatically retried. " + ex.Message).ConfigureAwait(false);
                        Logging.Append($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] PIPE WRITE STOPPED: method={job.Method} jobId={job.JobId} code=PIPE_DRY_RUN_OUTCOME_UNKNOWN autoRetry=false");
                    }
                    catch (Exception deadEx)
                    {
                        _logger.LogError(deadEx, "Failed to stop ambiguous NP-3B dry-run job {JobId}.", job.JobId);
                    }
                }
                else
                {
                    try { await _durable.RequeueAsync(job.JobId).ConfigureAwait(false); }
                    catch (Exception requeueEx) { _logger.LogWarning(requeueEx, "Failed to requeue safe named-pipe job {JobId}.", job.JobId); }
                }
            }
            throw;
        }
        finally
        {
            _pending.TryRemove(requestId, out _);
        }
    }

    private async Task WriteFrameAsync(NamedPipeServerStream pipe, string json, CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!pipe.IsConnected) throw new IOException("Named-pipe session is disconnected.");
            await NamedPipeFrameCodec.WriteJsonAsync(pipe, json, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private void CancelPending(Exception exception)
    {
        foreach (var pair in _pending.ToArray())
        {
            if (_pending.TryRemove(pair.Key, out var pending))
            {
                pending.Accepted.TrySetException(exception);
                pending.Response.TrySetException(exception);
            }
        }
    }

    private static async Task WaitWithTimeoutAsync(
        Task task,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        string timeoutMessage = "Named-pipe command acceptance timed out.")
    {
        var delay = Task.Delay(timeout, cancellationToken);
        var completed = await Task.WhenAny(task, delay).ConfigureAwait(false);
        if (completed != task)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new TimeoutException(timeoutMessage);
        }
        await task.ConfigureAwait(false);
    }

    private static long ElapsedBetween(DateTimeOffset from, DateTimeOffset? to)
        => to.HasValue ? Math.Max(0, (long)(to.Value - from).TotalMilliseconds) : -1;

    private static TimeSpan RequirePositive(TimeSpan value, string parameterName)
    {
        if (value <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(parameterName, "Named-pipe timing values must be positive.");
        return value;
    }

    private static async Task<string> ReadWithTimeoutAsync(
        Stream stream,
        TimeSpan timeout,
        CancellationToken stoppingToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeoutCts.CancelAfter(timeout);
        return await NamedPipeFrameCodec.ReadJsonAsync(stream, timeoutCts.Token).ConfigureAwait(false);
    }

    private void SetStatus(string status, bool clearClient)
    {
        lock (_stateGate)
        {
            _status = status;
            if (clearClient)
            {
                _activePipe = null;
                _revitProcessId = 0;
                _revitVersion = string.Empty;
                _bridgeTimingFractionalSecondsPreserved = false;
                _writeRecoveryReady = false;
                _connectedUtc = null;
            }
        }
    }

    private static string GetString(JsonElement root, string name)
    {
        return root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? string.Empty
            : string.Empty;
    }

    private static int GetInt32(JsonElement root, string name)
    {
        return root.TryGetProperty(name, out var value) && value.TryGetInt32(out var result)
            ? result
            : 0;
    }

    private static bool GetBoolean(JsonElement root, string name)
    {
        return root.TryGetProperty(name, out var value)
            && (value.ValueKind == JsonValueKind.True
                || (value.ValueKind == JsonValueKind.String
                    && bool.TryParse(value.GetString(), out var parsed)
                    && parsed));
    }

    private sealed class ReceiptRecoverySession
    {
        private readonly object _gate = new();
        private readonly int _declaredReceiptCount;
        private string _batchId = string.Empty;
        private bool _begun;
        private bool _ended;
        private int _processedCount;
        private int _rejectedCount;

        public ReceiptRecoverySession(bool enabled, int declaredReceiptCount)
        {
            Enabled = enabled;
            _declaredReceiptCount = Math.Max(0, declaredReceiptCount);
        }

        public bool Enabled { get; }
        public TaskCompletionSource<bool> Completed { get; }
            = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool Begun
        {
            get { lock (_gate) return _begun; }
        }

        public bool HasRejected
        {
            get { lock (_gate) return _rejectedCount > 0; }
        }

        public int ProcessedCount
        {
            get { lock (_gate) return _processedCount; }
        }

        public int RejectedCount
        {
            get { lock (_gate) return _rejectedCount; }
        }

        public void Begin(string batchId, int receiptCount)
        {
            lock (_gate)
            {
                if (!Enabled) throw new InvalidDataException("Receipt recovery is disabled.");
                if (_begun) throw new InvalidDataException("Receipt recovery was already started.");
                if (string.IsNullOrWhiteSpace(batchId)) throw new InvalidDataException("Receipt recovery batchId is required.");
                if (receiptCount != _declaredReceiptCount)
                    throw new InvalidDataException("Receipt recovery count does not match the handshake declaration.");

                _batchId = batchId;
                _begun = true;
            }
        }

        public void RecordProcessed(bool accepted)
        {
            lock (_gate)
            {
                if (!_begun || _ended) throw new InvalidDataException("Receipt recovery item arrived outside an active batch.");
                _processedCount++;
                if (!accepted) _rejectedCount++;
                if (_processedCount > _declaredReceiptCount)
                    throw new InvalidDataException("Receipt recovery contained more items than declared.");
            }
        }

        public void End(string batchId, int sentCount)
        {
            lock (_gate)
            {
                if (!_begun || _ended) throw new InvalidDataException("Receipt recoveryEnd arrived outside an active batch.");
                if (!string.Equals(_batchId, batchId, StringComparison.Ordinal))
                    throw new InvalidDataException("Receipt recovery batchId mismatch.");
                if (sentCount != _declaredReceiptCount || sentCount != _processedCount)
                    throw new InvalidDataException("Receipt recovery count mismatch at recoveryEnd.");

                _ended = true;
                Completed.TrySetResult(true);
            }
        }
    }

    private sealed class PendingPipeJob
    {
        public PendingPipeJob(string jobId, string rpcId, string method)
        {
            JobId = jobId;
            RpcId = rpcId;
            Method = method;
        }

        public string JobId { get; }
        public string RpcId { get; }
        public string Method { get; }
        public DateTimeOffset CreatedUtc { get; } = DateTimeOffset.UtcNow;
        public DateTimeOffset? AcceptedUtc { get; set; }
        public DateTimeOffset? StartedUtc { get; set; }
        public TaskCompletionSource<bool> Accepted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<PipeJobResponse> Response { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class PipeJobResponse
    {
        public PipeJobResponse(bool ok, string resultJson, string code, string message)
        {
            Ok = ok;
            ResultJson = resultJson;
            Code = code;
            Message = message;
        }

        public bool Ok { get; }
        public string ResultJson { get; }
        public string Code { get; }
        public string Message { get; }
    }
}

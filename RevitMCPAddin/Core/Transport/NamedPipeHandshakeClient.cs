#nullable enable
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RevitMCP.Abstractions.Bridge;
using RevitMCP.Abstractions.Json;
using RevitMCPAddin.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RevitMCPAddin.Core.Transport
{
    internal sealed class NamedPipeReadOnlyCommand
    {
        public string RequestId { get; set; } = string.Empty;
        public string JobId { get; set; } = string.Empty;
        public string RpcId { get; set; } = string.Empty;
        public string Method { get; set; } = string.Empty;
        public string CommandClass { get; set; } = NamedPipeCommandPolicy.ReadOnlyClass;
        public string TransportName { get; set; } = "namedPipeReadOnly";
        public JObject Params { get; set; } = new JObject();
        public int TimeoutMs { get; set; }
        public DateTime ReceivedAtUtc { get; set; } = DateTime.UtcNow;
        public DateTime? ServerSentUtc { get; set; }
    }

    /// <summary>
    /// NP-1 connection monitor, NP-2 read-only receiver, and NP-3B persistent
    /// receipt recovery for the explicit dry-run write receiver. Apply
    /// commands remain on legacy HTTP.
    /// </summary>
    internal sealed class NamedPipeHandshakeClient
    {
        private readonly object _baseGate = new object();
        private readonly HttpClient _http;
        private readonly int _revitProcessId;
        private readonly string _revitVersion;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private string _baseAddress;
        private Task? _loopTask;
        private string _lastLoggedStatus = string.Empty;
        private readonly object _sessionGate = new object();
        private PipeSession? _activeSession;
        private bool _writeDryRunDispatchEnabled;
        private bool _writeReceiptRecoveryEnabled;
        private readonly object _journalGate = new object();
        private NamedPipeWriteReceiptJournal? _receiptJournal;
        private int _receiptJournalPort;
        private readonly ConcurrentDictionary<string, PipeCommandReceipt> _receipts
            = new ConcurrentDictionary<string, PipeCommandReceipt>(StringComparer.Ordinal);

        public Func<NamedPipeReadOnlyCommand, bool>? CommandReceived { get; set; }

        public NamedPipeHandshakeClient(string baseAddress, string revitVersion)
        {
            _baseAddress = NormalizeBaseAddress(baseAddress);
            _revitProcessId = Process.GetCurrentProcess().Id;
            _revitVersion = revitVersion ?? string.Empty;
            _http = new HttpClient(new HttpClientHandler
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                UseCookies = false,
                Proxy = null,
                UseProxy = false,
                AllowAutoRedirect = false
            })
            {
                Timeout = TimeSpan.FromSeconds(8)
            };
        }

        public void Start()
        {
            if (_loopTask != null) return;
            _loopTask = Task.Run(() => RunLoopAsync(_cts.Token));
        }

        public void Stop()
        {
            try { _cts.Cancel(); } catch { }
            try { RevitInstanceStateService.SetBridgeState(0, string.Empty, NamedPipeFrameCodec.ProtocolVersion, "stopped", 0); } catch { }
        }

        public void SetBaseAddress(string baseAddress)
        {
            lock (_baseGate) _baseAddress = NormalizeBaseAddress(baseAddress);
        }

        public bool TryQueueStarted(
            string requestId,
            string jobId,
            string rpcId,
            long externalEventWaitMs)
        {
            var frame = JsonConvert.SerializeObject(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "started",
                requestId = requestId ?? string.Empty,
                jobId = jobId ?? string.Empty,
                rpcId = rpcId ?? string.Empty,
                ok = true,
                externalEventWaitMs = Math.Max(0, externalEventWaitMs),
                startedUtc = DateTimeOffset.UtcNow.ToString("o")
            });
            if (_receipts.TryGetValue(jobId ?? string.Empty, out var receipt))
            {
                receipt.Started = true;
                if (receipt.IsDryRunWrite)
                {
                    try { GetReceiptJournal()?.AppendStarted(receipt.JobId); }
                    catch (Exception ex)
                    {
                        RevitLogger.Warn("[BRIDGE] persistent receipt start update failed jobId="
                            + receipt.JobId + " error=" + ex.Message);
                    }
                }
            }
            return TryQueueFrame(frame);
        }

        public bool TryQueueResult(
            string requestId,
            string jobId,
            string rpcId,
            string resultJson,
            long externalEventWaitMs,
            long addinExecutionToResultMs)
        {
            var key = jobId ?? string.Empty;
            PipeCommandReceipt? receipt = null;
            if (!string.IsNullOrWhiteSpace(key))
            {
                receipt = _receipts.GetOrAdd(
                    key,
                    _ => new PipeCommandReceipt(requestId, key, rpcId, string.Empty, string.Empty, string.Empty));
                if (receipt.IsDryRunWrite)
                {
                    try
                    {
                        GetReceiptJournal()?.AppendCompleted(
                            receipt.JobId,
                            resultJson ?? string.Empty,
                            externalEventWaitMs,
                            addinExecutionToResultMs);
                    }
                    catch (Exception ex)
                    {
                        // The already persisted accepted/started state still
                        // prevents automatic replay after a crash.
                        RevitLogger.Warn("[BRIDGE] persistent receipt completion update failed jobId="
                            + receipt.JobId + " error=" + ex.Message);
                    }
                }
            }

            var frame = BuildResultFrame(
                requestId ?? string.Empty,
                key,
                rpcId ?? string.Empty,
                resultJson ?? string.Empty,
                externalEventWaitMs,
                addinExecutionToResultMs,
                recovered: false);
            if (receipt != null)
            {
                receipt.ResultFrame = frame;
                receipt.Completed = true;
            }
            return TryQueueFrame(frame);
        }

        private bool TryQueueFrame(string json)
        {
            PipeSession? session;
            lock (_sessionGate) session = _activeSession;
            if (session == null || !session.Pipe.IsConnected || session.Cancellation.IsCancellationRequested)
                return false;

            session.Outbound.Enqueue(json ?? string.Empty);
            try { session.Signal.Release(); }
            catch { return false; }
            return true;
        }

        private async Task RunLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                string pipeName = string.Empty;
                int serverProcessId = 0;
                int port = 0;
                try
                {
                    var descriptor = await GetDescriptorAsync(cancellationToken).ConfigureAwait(false);
                    var enabled = descriptor.Value<bool?>("enabled") ?? false;
                    port = descriptor.Value<int?>("port") ?? 0;
                    if (!enabled)
                    {
                        SetState(port, string.Empty, "disabled", descriptor.Value<int?>("serverProcessId") ?? 0);
                        await DelayAsync(TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    var receiptJournal = EnsureReceiptJournal(port);
                    IReadOnlyList<NamedPipeWriteReceipt> recoveryReceipts = receiptJournal != null
                        ? receiptJournal.GetUnresolved()
                        : Array.Empty<NamedPipeWriteReceipt>();

                    var protocol = descriptor.Value<string>("protocol") ?? string.Empty;
                    pipeName = descriptor.Value<string>("pipeName") ?? string.Empty;
                    var nonce = descriptor.Value<string>("nonce") ?? string.Empty;
                    serverProcessId = descriptor.Value<int?>("serverProcessId") ?? 0;
                    if (!string.Equals(protocol, NamedPipeFrameCodec.ProtocolVersion, StringComparison.Ordinal)
                        || string.IsNullOrWhiteSpace(pipeName)
                        || string.IsNullOrWhiteSpace(nonce))
                    {
                        throw new InvalidOperationException("Named-pipe bridge descriptor was incomplete or incompatible.");
                    }

                    using (var pipe = new NamedPipeClientStream(
                        ".",
                        pipeName,
                        PipeDirection.InOut,
                        PipeOptions.Asynchronous))
                    {
                        await pipe.ConnectAsync(3000, cancellationToken).ConfigureAwait(false);
                        var requestId = Guid.NewGuid().ToString("N");
                        var hello = JsonConvert.SerializeObject(new
                        {
                            protocol = NamedPipeFrameCodec.ProtocolVersion,
                            kind = "hello",
                            requestId,
                            nonce,
                            revitProcessId = _revitProcessId,
                            revitVersion = _revitVersion,
                            connectedUtc = DateTimeOffset.UtcNow.ToString("o"),
                            commandTransport = "legacyHttp",
                            bridgeTimingFractionalSecondsPreserved = true,
                            writeReceiptRecoverySupported = receiptJournal != null,
                            unresolvedWriteReceiptCount = recoveryReceipts.Count
                        });
                        await NamedPipeFrameCodec.WriteJsonAsync(pipe, hello, cancellationToken).ConfigureAwait(false);

                        var ackText = await ReadWithTimeoutAsync(pipe, TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
                        var ack = JObject.Parse(ackText);
                        if (!(ack.Value<bool?>("ok") ?? false)
                            || !string.Equals(ack.Value<string>("kind"), "helloAck", StringComparison.Ordinal)
                            || !string.Equals(ack.Value<string>("requestId"), requestId, StringComparison.Ordinal)
                            || !string.Equals(ack.Value<string>("protocol"), NamedPipeFrameCodec.ProtocolVersion, StringComparison.Ordinal))
                        {
                            throw new InvalidOperationException("Named-pipe bridge rejected the NP-1 handshake.");
                        }

                        var readOnlyEnabled = ack.Value<bool?>("readOnlyDispatchEnabled") ?? false;
                        _writeDryRunDispatchEnabled = ack.Value<bool?>("writeDryRunDispatchEnabled") ?? false;
                        _writeReceiptRecoveryEnabled = ack.Value<bool?>("writeReceiptRecoveryEnabled") ?? false;
                        SetState(
                            port,
                            pipeName,
                            _writeDryRunDispatchEnabled
                                ? "connected-dryrun-np3b"
                                : (readOnlyEnabled ? "connected-readonly-np2" : "connected-handshake-only"),
                            serverProcessId);

                        using (var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                        {
                            var session = new PipeSession(pipe, sessionCts);
                            lock (_sessionGate) _activeSession = session;
                            Task? writerTask = null;
                            Task? pingTask = null;
                            Task? readerTask = null;
                            try
                            {
                                writerTask = WriterLoopAsync(session, sessionCts.Token);
                                pingTask = PingLoopAsync(session, sessionCts.Token);
                                readerTask = ReaderLoopAsync(session, sessionCts.Token);
                                if (_writeReceiptRecoveryEnabled)
                                    QueueReceiptRecoveryBatch(session, recoveryReceipts);
                                var completed = await Task.WhenAny(writerTask, pingTask, readerTask).ConfigureAwait(false);
                                await completed.ConfigureAwait(false);
                            }
                            finally
                            {
                                lock (_sessionGate)
                                {
                                    if (ReferenceEquals(_activeSession, session)) _activeSession = null;
                                }
                                _writeDryRunDispatchEnabled = false;
                                _writeReceiptRecoveryEnabled = false;
                                try { sessionCts.Cancel(); } catch { }
                                try { session.Signal.Release(); } catch { }
                                try
                                {
                                    var tasks = new[] { writerTask, pingTask, readerTask };
                                    foreach (var task in tasks)
                                    {
                                        if (task == null) continue;
                                        try { await task.ConfigureAwait(false); }
                                        catch (OperationCanceledException) { }
                                        catch { }
                                    }
                                }
                                catch { }
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Preserve the stack trace in the Revit-side log. A message-only
                    // fallback hid the exact Json.NET compatibility call during NP-2.1.
                    SetState(port, pipeName, "legacyHttpFallback", serverProcessId, ex.ToString());
                    try { await DelayAsync(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException) { break; }
                }
            }
        }

        private async Task WriterLoopAsync(PipeSession session, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await session.Signal.WaitAsync(cancellationToken).ConfigureAwait(false);
                while (session.Outbound.TryDequeue(out var json))
                {
                    await NamedPipeFrameCodec.WriteJsonAsync(session.Pipe, json, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task PingLoopAsync(PipeSession session, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await DelayAsync(TimeSpan.FromSeconds(15), cancellationToken).ConfigureAwait(false);
                if (!TryQueueFrame(JsonConvert.SerializeObject(new
                {
                    protocol = NamedPipeFrameCodec.ProtocolVersion,
                    kind = "ping",
                    requestId = Guid.NewGuid().ToString("N"),
                    clientUtc = DateTimeOffset.UtcNow.ToString("o")
                })))
                {
                    throw new IOException("Named-pipe ping could not be queued.");
                }
            }
        }

        private async Task ReaderLoopAsync(PipeSession session, CancellationToken cancellationToken)
        {
            while (session.Pipe.IsConnected && !cancellationToken.IsCancellationRequested)
            {
                var text = await NamedPipeFrameCodec.ReadJsonAsync(session.Pipe, cancellationToken).ConfigureAwait(false);
                var message = JObject.Parse(text);
                if (!string.Equals(message.Value<string>("protocol"), NamedPipeFrameCodec.ProtocolVersion, StringComparison.Ordinal))
                    throw new InvalidDataException("Named-pipe message protocol mismatch.");

                var kind = message.Value<string>("kind") ?? string.Empty;
                var requestId = message.Value<string>("requestId") ?? string.Empty;
                if (string.Equals(kind, "pong", StringComparison.Ordinal))
                    continue;

                if (string.Equals(kind, "recoveryAck", StringComparison.Ordinal))
                {
                    HandleRecoveryAck(message, requestId);
                    continue;
                }

                if (string.Equals(kind, "recoveryBatchAck", StringComparison.Ordinal))
                {
                    var ok = message.Value<bool?>("ok") ?? false;
                    RevitLogger.Info("[BRIDGE] NP-3B receipt recovery batch acknowledged ok=" + ok
                        + " processed=" + (message.Value<int?>("processedCount") ?? 0)
                        + " rejected=" + (message.Value<int?>("rejectedCount") ?? 0)
                        + " code=" + (message.Value<string>("code") ?? string.Empty));
                    continue;
                }

                if (string.Equals(kind, "request", StringComparison.Ordinal))
                {
                    HandlePipeRequest(message, requestId);
                    continue;
                }

                if (string.Equals(kind, "ack", StringComparison.Ordinal))
                {
                    var jobId = message.Value<string>("jobId") ?? requestId;
                    var canRemove = true;
                    if (_receipts.TryGetValue(jobId, out var receipt) && receipt.IsDryRunWrite)
                    {
                        try { GetReceiptJournal()?.AppendAcknowledged(jobId, "ACK"); }
                        catch (Exception ex)
                        {
                            canRemove = false;
                            RevitLogger.Warn("[BRIDGE] persistent receipt ACK update failed jobId="
                                + jobId + " error=" + ex.Message);
                        }
                    }
                    else
                    {
                        try
                        {
                            var journal = GetReceiptJournal();
                            if (journal != null && journal.TryGet(jobId, out _))
                                journal.AppendAcknowledged(jobId, "ACK");
                        }
                        catch (Exception ex)
                        {
                            canRemove = false;
                            RevitLogger.Warn("[BRIDGE] persistent receipt ACK recovery failed jobId="
                                + jobId + " error=" + ex.Message);
                        }
                    }
                    if (canRemove) _receipts.TryRemove(jobId, out _);
                    RevitLogger.Info("[BRIDGE] result acknowledged requestId=" + requestId + " jobId=" + jobId);
                    continue;
                }

                if (string.Equals(kind, "error", StringComparison.Ordinal))
                {
                    RevitLogger.Warn("[BRIDGE] named-pipe error requestId=" + requestId
                        + " code=" + (message.Value<string>("code") ?? string.Empty)
                        + " msg=" + (message.Value<string>("msg") ?? string.Empty));
                    continue;
                }
            }
        }

        private void HandlePipeRequest(JObject message, string requestId)
        {
            var method = message.Value<string>("method") ?? string.Empty;
            var jobId = message.Value<string>("jobId") ?? requestId;
            var rpcId = message.Value<string>("rpcId") ?? requestId;
            var parameters = message["params"] as JObject ?? new JObject();
            var paramsJson = JsonNetHostCompat.ToCompactJson(parameters);
            var paramsSha256 = NamedPipeReceiptHash.ComputeCanonicalJsonSha256(paramsJson);
            var declaredParamsSha256 = message.Value<string>("paramsSha256") ?? string.Empty;
            var isReadOnly = NamedPipeReadOnlyAllowlist.Contains(method);
            var isDryRunWrite = _writeDryRunDispatchEnabled
                && NamedPipeCommandPolicy.IsExplicitDryRunWrite(
                    method,
                    paramsJson);
            var commandClass = isDryRunWrite
                ? NamedPipeCommandPolicy.DryRunWriteClass
                : (isReadOnly ? NamedPipeCommandPolicy.ReadOnlyClass : string.Empty);
            var declaredClass = message.Value<string>("commandClass") ?? string.Empty;
            if (string.IsNullOrEmpty(commandClass)
                || (!string.IsNullOrWhiteSpace(declaredClass)
                    && !string.Equals(declaredClass, commandClass, StringComparison.Ordinal)))
            {
                TryQueueFrame(JsonConvert.SerializeObject(new
                {
                    protocol = NamedPipeFrameCodec.ProtocolVersion,
                    kind = "response",
                    requestId,
                    jobId,
                    rpcId,
                    ok = false,
                    code = "PIPE_COMMAND_POLICY_REJECTED",
                    msg = "The command is not an allowed read, or is not an explicitly enabled dryRun:true write."
                }));
                return;
            }

            if (isDryRunWrite
                && !NamedPipeReceiptHash.FixedTimeEquals(paramsSha256, declaredParamsSha256))
            {
                TryQueueFrame(BuildRejectedResponseFrame(
                    requestId,
                    jobId,
                    rpcId,
                    "PIPE_PARAMS_HASH_MISMATCH",
                    "The canonical params hash did not match the Server request."));
                return;
            }

            if (_receipts.TryGetValue(jobId, out var previous))
            {
                if (!previous.Matches(method, paramsSha256))
                {
                    TryQueueFrame(BuildRejectedResponseFrame(
                        requestId,
                        jobId,
                        rpcId,
                        "PIPE_JOB_ID_COLLISION",
                        "An in-memory receipt with the same jobId has a different method or params hash."));
                    return;
                }
                if (previous.Completed && !string.IsNullOrWhiteSpace(previous.ResultFrame))
                {
                    TryQueueFrame(previous.ResultFrame);
                    RevitLogger.Warn("[BRIDGE] duplicate request replayed cached result without Revit execution jobId=" + jobId);
                }
                else
                {
                    TryQueueFrame(BuildAcceptedFrame(
                        requestId,
                        jobId,
                        rpcId,
                        DateTime.UtcNow,
                        null,
                        duplicate: true));
                    RevitLogger.Warn("[BRIDGE] duplicate request ignored while original is pending jobId=" + jobId);
                }
                return;
            }

            var receiptJournal = GetReceiptJournal();
            if (isDryRunWrite && receiptJournal == null)
            {
                TryQueueFrame(BuildRejectedResponseFrame(
                    requestId,
                    jobId,
                    rpcId,
                    "PIPE_RECEIPT_JOURNAL_UNAVAILABLE",
                    "The persistent Add-in receipt journal is unavailable; dry-run write execution was refused."));
                return;
            }

            if (isDryRunWrite
                && receiptJournal != null
                && receiptJournal.TryGet(jobId, out var persisted))
            {
                if (!string.Equals(persisted.Method, method, StringComparison.Ordinal)
                    || !NamedPipeReceiptHash.FixedTimeEquals(persisted.ParamsSha256, paramsSha256))
                {
                    TryQueueFrame(BuildRejectedResponseFrame(
                        requestId,
                        jobId,
                        rpcId,
                        "PIPE_PERSISTED_JOB_ID_COLLISION",
                        "A persisted receipt with the same jobId has a different method or params hash."));
                    return;
                }

                if (!string.IsNullOrWhiteSpace(persisted.ResultJson)
                    && (string.Equals(persisted.State, NamedPipeWriteReceiptStates.Completed, StringComparison.Ordinal)
                        || string.Equals(persisted.State, NamedPipeWriteReceiptStates.RecoveryRejected, StringComparison.Ordinal)))
                {
                    TryQueueFrame(BuildResultFrame(
                        requestId,
                        jobId,
                        rpcId,
                        persisted.ResultJson,
                        persisted.ExternalEventWaitMs,
                        persisted.AddinExecutionToResultMs,
                        recovered: true));
                    RevitLogger.Warn("[BRIDGE] persisted result replayed without Revit execution jobId=" + jobId);
                }
                else
                {
                    TryQueueFrame(BuildRejectedResponseFrame(
                        requestId,
                        jobId,
                        rpcId,
                        "PIPE_WRITE_RECEIPT_UNRESOLVED",
                        "A persisted accepted/started receipt exists. Automatic Revit re-execution is disabled."));
                }
                return;
            }

            var serverSentUtc = NamedPipeTimestamp.ReadUtc(message["serverSentUtc"]);
            var receipt = new PipeCommandReceipt(
                requestId,
                jobId,
                rpcId,
                method,
                commandClass,
                paramsSha256);
            if (isDryRunWrite)
            {
                try
                {
                    receiptJournal!.AppendAccepted(
                        requestId,
                        jobId,
                        rpcId,
                        method,
                        commandClass,
                        paramsSha256,
                        _receiptJournalPort,
                        _revitProcessId,
                        _revitVersion);
                }
                catch (Exception ex)
                {
                    TryQueueFrame(BuildRejectedResponseFrame(
                        requestId,
                        jobId,
                        rpcId,
                        "PIPE_RECEIPT_PERSIST_FAILED",
                        "The request was not queued because its durable receipt could not be written. " + ex.Message));
                    return;
                }
            }
            if (!_receipts.TryAdd(jobId, receipt))
            {
                TryQueueFrame(JsonConvert.SerializeObject(new
                {
                    protocol = NamedPipeFrameCodec.ProtocolVersion,
                    kind = "response",
                    requestId,
                    jobId,
                    rpcId,
                    ok = false,
                    code = "PIPE_DUPLICATE_REQUEST_RACE",
                    msg = "A request with the same jobId was accepted concurrently."
                }));
                return;
            }

            var command = new NamedPipeReadOnlyCommand
            {
                RequestId = requestId,
                JobId = jobId,
                RpcId = rpcId,
                Method = method,
                CommandClass = commandClass,
                TransportName = isDryRunWrite ? "namedPipeDryRun" : "namedPipeReadOnly",
                Params = parameters,
                TimeoutMs = Math.Max(10_000, message.Value<int?>("timeoutMs") ?? 120_000),
                ReceivedAtUtc = DateTime.UtcNow,
                ServerSentUtc = serverSentUtc
            };

            bool accepted = false;
            string error = string.Empty;
            try
            {
                accepted = CommandReceived?.Invoke(command) ?? false;
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }

            if (!accepted)
            {
                if (isDryRunWrite)
                {
                    try
                    {
                        receiptJournal?.AppendRejected(
                            jobId,
                            "PIPE_ADDIN_REJECTED_COMMAND",
                            string.IsNullOrWhiteSpace(error) ? "The Revit worker rejected the command before execution." : error);
                    }
                    catch (Exception ex)
                    {
                        RevitLogger.Warn("[BRIDGE] persistent receipt reject update failed jobId="
                            + jobId + " error=" + ex.Message);
                    }
                }
                _receipts.TryRemove(jobId, out _);
                TryQueueFrame(JsonConvert.SerializeObject(new
                {
                    protocol = NamedPipeFrameCodec.ProtocolVersion,
                    kind = "response",
                    requestId,
                    jobId,
                    rpcId,
                    ok = false,
                    code = "PIPE_ADDIN_REJECTED_COMMAND",
                    msg = string.IsNullOrWhiteSpace(error) ? "The Revit worker rejected the named-pipe command." : error
                }));
                return;
            }

            TryQueueFrame(BuildAcceptedFrame(
                requestId,
                jobId,
                rpcId,
                command.ReceivedAtUtc,
                command.ServerSentUtc,
                duplicate: false));
        }

        private static string BuildAcceptedFrame(
            string requestId,
            string jobId,
            string rpcId,
            DateTime receivedAtUtc,
            DateTime? serverSentUtc,
            bool duplicate)
        {
            return JsonConvert.SerializeObject(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "accepted",
                requestId,
                jobId,
                rpcId,
                ok = true,
                duplicate,
                receivedUtc = receivedAtUtc.ToString("o"),
                serverToAddinMs = serverSentUtc.HasValue
                    ? Math.Max(0, (long)(receivedAtUtc - serverSentUtc.Value).TotalMilliseconds)
                    : -1L
            });
        }

        private static string BuildResultFrame(
            string requestId,
            string jobId,
            string rpcId,
            string resultJson,
            long externalEventWaitMs,
            long addinExecutionToResultMs,
            bool recovered)
        {
            return JsonConvert.SerializeObject(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "response",
                requestId = requestId ?? string.Empty,
                jobId = jobId ?? string.Empty,
                rpcId = rpcId ?? string.Empty,
                ok = true,
                resultJson = resultJson ?? string.Empty,
                recovered,
                reexecuted = false,
                timings = new
                {
                    externalEventWaitMs = Math.Max(0, externalEventWaitMs),
                    addinExecutionToResultMs = Math.Max(0, addinExecutionToResultMs)
                },
                completedUtc = DateTimeOffset.UtcNow.ToString("o")
            });
        }

        private static string BuildRejectedResponseFrame(
            string requestId,
            string jobId,
            string rpcId,
            string code,
            string message)
        {
            return JsonConvert.SerializeObject(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "response",
                requestId = requestId ?? string.Empty,
                jobId = jobId ?? string.Empty,
                rpcId = rpcId ?? string.Empty,
                ok = false,
                code = code ?? "PIPE_COMMAND_REJECTED",
                msg = message ?? string.Empty,
                reexecuted = false
            });
        }

        private NamedPipeWriteReceiptJournal? EnsureReceiptJournal(int port)
        {
            if (port <= 0 || port >= 65536) return null;
            lock (_journalGate)
            {
                if (_receiptJournal != null && _receiptJournalPort == port)
                    return _receiptJournal;
                try
                {
                    _receiptJournal = new NamedPipeWriteReceiptJournal(
                        NamedPipeWriteReceiptJournal.BuildDefaultPath(port));
                    _receiptJournalPort = port;
                    if (_receiptJournal.IgnoredCorruptLineCount > 0)
                    {
                        RevitLogger.Warn("[BRIDGE] persistent receipt journal ignored corrupt/partial lines count="
                            + _receiptJournal.IgnoredCorruptLineCount
                            + " path=" + _receiptJournal.FilePath);
                    }
                    return _receiptJournal;
                }
                catch (Exception ex)
                {
                    _receiptJournal = null;
                    _receiptJournalPort = 0;
                    RevitLogger.Warn("[BRIDGE] persistent receipt journal unavailable; dry-run writes will be refused. "
                        + ex.Message);
                    return null;
                }
            }
        }

        private NamedPipeWriteReceiptJournal? GetReceiptJournal()
        {
            lock (_journalGate) return _receiptJournal;
        }

        private void QueueReceiptRecoveryBatch(
            PipeSession session,
            IReadOnlyList<NamedPipeWriteReceipt> receipts)
        {
            var batchId = Guid.NewGuid().ToString("N");
            QueueSessionFrame(session, JsonConvert.SerializeObject(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "recoveryBegin",
                requestId = Guid.NewGuid().ToString("N"),
                batchId,
                receiptCount = receipts?.Count ?? 0,
                clientUtc = DateTimeOffset.UtcNow.ToString("o")
            }));

            var sentCount = 0;
            if (receipts != null)
            {
                foreach (var receipt in receipts)
                {
                    var hasResult = !string.IsNullOrWhiteSpace(receipt.ResultJson)
                        && !string.IsNullOrWhiteSpace(receipt.ResultSha256);
                    var frame = hasResult
                        ? JsonConvert.SerializeObject(new
                        {
                            protocol = NamedPipeFrameCodec.ProtocolVersion,
                            kind = "recoveryResponse",
                            requestId = Guid.NewGuid().ToString("N"),
                            batchId,
                            jobId = receipt.JobId,
                            method = receipt.Method,
                            paramsSha256 = receipt.ParamsSha256,
                            resultSha256 = receipt.ResultSha256,
                            resultJson = receipt.ResultJson,
                            receiptState = receipt.State,
                            completedUtc = receipt.UpdatedUtc
                        })
                        : JsonConvert.SerializeObject(new
                        {
                            protocol = NamedPipeFrameCodec.ProtocolVersion,
                            kind = "recoveryUnknown",
                            requestId = Guid.NewGuid().ToString("N"),
                            batchId,
                            jobId = receipt.JobId,
                            method = receipt.Method,
                            paramsSha256 = receipt.ParamsSha256,
                            receiptState = receipt.State,
                            updatedUtc = receipt.UpdatedUtc
                        });
                    QueueSessionFrame(session, frame);
                    sentCount++;
                }
            }

            QueueSessionFrame(session, JsonConvert.SerializeObject(new
            {
                protocol = NamedPipeFrameCodec.ProtocolVersion,
                kind = "recoveryEnd",
                requestId = Guid.NewGuid().ToString("N"),
                batchId,
                sentCount,
                clientUtc = DateTimeOffset.UtcNow.ToString("o")
            }));
            RevitLogger.Info("[BRIDGE] NP-3B persistent receipt recovery queued count=" + sentCount);
        }

        private static void QueueSessionFrame(PipeSession session, string frame)
        {
            if (session == null || session.Cancellation.IsCancellationRequested || !session.Pipe.IsConnected)
                throw new IOException("Named-pipe recovery frame could not be queued.");
            session.Outbound.Enqueue(frame ?? string.Empty);
            session.Signal.Release();
        }

        private void HandleRecoveryAck(JObject message, string requestId)
        {
            var jobId = message.Value<string>("jobId") ?? string.Empty;
            var ok = message.Value<bool?>("ok") ?? false;
            var code = message.Value<string>("code") ?? string.Empty;
            var msg = message.Value<string>("msg") ?? string.Empty;
            var journal = GetReceiptJournal();
            try
            {
                if (journal != null && journal.TryGet(jobId, out _))
                {
                    if (ok) journal.AppendAcknowledged(jobId, code);
                    else journal.AppendRecoveryRejected(jobId, code, msg);
                }
                if (ok) _receipts.TryRemove(jobId, out _);
            }
            catch (Exception ex)
            {
                RevitLogger.Warn("[BRIDGE] recovery ACK persistence failed requestId=" + requestId
                    + " jobId=" + jobId + " error=" + ex.Message);
                return;
            }

            var log = "[BRIDGE] recovery ACK requestId=" + requestId + " jobId=" + jobId
                + " ok=" + ok + " code=" + code + " reexecuted=false";
            if (ok) RevitLogger.Info(log); else RevitLogger.Warn(log + " msg=" + msg);
        }

        private async Task<JObject> GetDescriptorAsync(CancellationToken cancellationToken)
        {
            string baseAddress;
            lock (_baseGate) baseAddress = _baseAddress;
            using (var response = await _http.GetAsync(baseAddress + "bridge/info", cancellationToken).ConfigureAwait(false))
            {
                var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw new HttpRequestException("GET /bridge/info failed: HTTP " + (int)response.StatusCode + ".");
                return JObject.Parse(text);
            }
        }

        private void SetState(
            int port,
            string pipeName,
            string status,
            int serverProcessId,
            string? detail = null)
        {
            try
            {
                RevitInstanceStateService.SetBridgeState(
                    port,
                    pipeName,
                    NamedPipeFrameCodec.ProtocolVersion,
                    status,
                    serverProcessId);
            }
            catch { }

            if (string.Equals(_lastLoggedStatus, status, StringComparison.Ordinal)) return;
            _lastLoggedStatus = status;
            if (string.Equals(status, "connected-handshake-only", StringComparison.Ordinal))
            {
                RevitLogger.Info("[BRIDGE] NP-1 named-pipe handshake connected; command transport remains legacyHttp.");
            }
            else if (string.Equals(status, "connected-readonly-np2", StringComparison.Ordinal))
            {
                RevitLogger.Info("[BRIDGE] NP-2.1 named-pipe read-only transport connected; methods="
                    + string.Join(",", NamedPipeReadOnlyAllowlist.CanonicalMethods)
                    + "; writes and legacy aliases remain legacyHttp.");
            }
            else if (string.Equals(status, "connected-dryrun-np3b", StringComparison.Ordinal))
            {
                RevitLogger.Info("[BRIDGE] NP-3B persistent-receipt dry-run transport connected; methods="
                    + string.Join(",", NamedPipeCommandPolicy.DryRunWriteMethods)
                    + "; append-only recovery is required; dryRun:false, omitted dryRun, aliases, and all apply commands remain legacyHttp.");
            }
            else if (string.Equals(status, "legacyHttpFallback", StringComparison.Ordinal))
            {
                RevitLogger.Warn("[BRIDGE] NP-1 named-pipe unavailable; legacyHttp remains active. " + (detail ?? string.Empty));
            }
            else
            {
                RevitLogger.Info("[BRIDGE] NP-1 status=" + status + "; command transport=legacyHttp.");
            }
        }

        private static async Task<string> ReadWithTimeoutAsync(
            NamedPipeClientStream pipe,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            using (var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                timeoutCts.CancelAfter(timeout);
                return await NamedPipeFrameCodec.ReadJsonAsync(pipe, timeoutCts.Token).ConfigureAwait(false);
            }
        }

        private static Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
        {
            return Task.Delay(delay, cancellationToken);
        }

        private static string NormalizeBaseAddress(string value)
        {
            var text = string.IsNullOrWhiteSpace(value) ? "http://127.0.0.1:5210/" : value.Trim();
            return text.EndsWith("/", StringComparison.Ordinal) ? text : text + "/";
        }

        private sealed class PipeSession
        {
            public PipeSession(NamedPipeClientStream pipe, CancellationTokenSource cancellation)
            {
                Pipe = pipe;
                Cancellation = cancellation;
            }

            public NamedPipeClientStream Pipe { get; }
            public CancellationTokenSource Cancellation { get; }
            public ConcurrentQueue<string> Outbound { get; } = new ConcurrentQueue<string>();
            public SemaphoreSlim Signal { get; } = new SemaphoreSlim(0);
        }

        private sealed class PipeCommandReceipt
        {
            public PipeCommandReceipt(
                string requestId,
                string jobId,
                string rpcId,
                string method,
                string commandClass,
                string paramsSha256)
            {
                RequestId = requestId ?? string.Empty;
                JobId = jobId ?? string.Empty;
                RpcId = rpcId ?? string.Empty;
                Method = method ?? string.Empty;
                CommandClass = commandClass ?? string.Empty;
                ParamsSha256 = paramsSha256 ?? string.Empty;
            }

            public string RequestId { get; }
            public string JobId { get; }
            public string RpcId { get; }
            public string Method { get; }
            public string CommandClass { get; }
            public string ParamsSha256 { get; }
            public bool IsDryRunWrite => string.Equals(
                CommandClass,
                NamedPipeCommandPolicy.DryRunWriteClass,
                StringComparison.Ordinal);
            public bool Started { get; set; }
            public bool Completed { get; set; }
            public string ResultFrame { get; set; } = string.Empty;

            public bool Matches(string method, string paramsSha256)
            {
                return string.Equals(Method, method ?? string.Empty, StringComparison.Ordinal)
                    && NamedPipeReceiptHash.FixedTimeEquals(ParamsSha256, paramsSha256);
            }
        }
    }
}

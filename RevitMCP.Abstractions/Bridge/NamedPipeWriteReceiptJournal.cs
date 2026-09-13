#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace RevitMCP.Abstractions.Bridge
{
    public static class NamedPipeWriteReceiptStates
    {
        public const string Accepted = "accepted";
        public const string Started = "started";
        public const string Completed = "completed";
        public const string Acknowledged = "acknowledged";
        public const string Rejected = "rejected";
        public const string RecoveryRejected = "recoveryRejected";
    }

    public sealed class NamedPipeWriteReceipt
    {
        public string SchemaVersion { get; set; } = NamedPipeWriteReceiptJournal.SchemaVersion;
        public long Sequence { get; set; }
        public string State { get; set; } = string.Empty;
        public string JobId { get; set; } = string.Empty;
        public string RequestId { get; set; } = string.Empty;
        public string RpcId { get; set; } = string.Empty;
        public string Method { get; set; } = string.Empty;
        public string CommandClass { get; set; } = string.Empty;
        public string ParamsSha256 { get; set; } = string.Empty;
        public string ResultSha256 { get; set; } = string.Empty;
        public string ResultJson { get; set; } = string.Empty;
        public string Code { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public int Port { get; set; }
        public int RevitProcessId { get; set; }
        public string RevitVersion { get; set; } = string.Empty;
        public string UpdatedUtc { get; set; } = DateTimeOffset.UtcNow.ToString("o");
        public long ExternalEventWaitMs { get; set; }
        public long AddinExecutionToResultMs { get; set; }

        public NamedPipeWriteReceipt Clone()
        {
            return new NamedPipeWriteReceipt
            {
                SchemaVersion = SchemaVersion,
                Sequence = Sequence,
                State = State,
                JobId = JobId,
                RequestId = RequestId,
                RpcId = RpcId,
                Method = Method,
                CommandClass = CommandClass,
                ParamsSha256 = ParamsSha256,
                ResultSha256 = ResultSha256,
                ResultJson = ResultJson,
                Code = Code,
                Message = Message,
                Port = Port,
                RevitProcessId = RevitProcessId,
                RevitVersion = RevitVersion,
                UpdatedUtc = UpdatedUtc,
                ExternalEventWaitMs = ExternalEventWaitMs,
                AddinExecutionToResultMs = AddinExecutionToResultMs
            };
        }
    }

    /// <summary>
    /// Append-only receipt journal for write-class pipe requests. The journal
    /// never deletes or truncates a file. A complete snapshot is appended for
    /// every state transition, allowing a partial final line to be ignored.
    /// </summary>
    public sealed class NamedPipeWriteReceiptJournal
    {
        public const string SchemaVersion = "revitmcp.pipeReceipt.v1";
        public const int MaximumPersistedResultBytes = 16 * 1024 * 1024;

        private readonly object _gate = new object();
        private readonly Dictionary<string, NamedPipeWriteReceipt> _unresolved
            = new Dictionary<string, NamedPipeWriteReceipt>(StringComparer.Ordinal);
        private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        private long _nextSequence = 1;

        public NamedPipeWriteReceiptJournal(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("A receipt journal path is required.", nameof(filePath));
            FilePath = Path.GetFullPath(filePath);
            LoadExisting();
        }

        public string FilePath { get; }
        public int IgnoredCorruptLineCount { get; private set; }

        public static string BuildDefaultPath(int port)
        {
            if (port <= 0 || port >= 65536) throw new ArgumentOutOfRangeException(nameof(port));
            var local = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            var directory = Path.Combine(local, "RevitMCP", "bridge_receipts");
            return Path.Combine(directory, "write_receipts_p" + port + ".jsonl");
        }

        public IReadOnlyList<NamedPipeWriteReceipt> GetUnresolved()
        {
            lock (_gate)
            {
                return _unresolved.Values
                    .OrderBy(x => x.Sequence)
                    .Select(x => x.Clone())
                    .ToArray();
            }
        }

        public bool TryGet(string jobId, out NamedPipeWriteReceipt receipt)
        {
            lock (_gate)
            {
                if (_unresolved.TryGetValue(jobId ?? string.Empty, out var current))
                {
                    receipt = current.Clone();
                    return true;
                }
            }

            receipt = new NamedPipeWriteReceipt();
            return false;
        }

        public NamedPipeWriteReceipt AppendAccepted(
            string requestId,
            string jobId,
            string rpcId,
            string method,
            string commandClass,
            string paramsSha256,
            int port,
            int revitProcessId,
            string revitVersion)
        {
            if (string.IsNullOrWhiteSpace(jobId)) throw new ArgumentException("jobId is required.", nameof(jobId));
            if (string.IsNullOrWhiteSpace(method)) throw new ArgumentException("method is required.", nameof(method));
            if (string.IsNullOrWhiteSpace(paramsSha256)) throw new ArgumentException("paramsSha256 is required.", nameof(paramsSha256));

            var receipt = new NamedPipeWriteReceipt
            {
                State = NamedPipeWriteReceiptStates.Accepted,
                RequestId = requestId ?? string.Empty,
                JobId = jobId,
                RpcId = rpcId ?? string.Empty,
                Method = method,
                CommandClass = commandClass ?? string.Empty,
                ParamsSha256 = paramsSha256,
                Port = port,
                RevitProcessId = revitProcessId,
                RevitVersion = revitVersion ?? string.Empty
            };
            return AppendSnapshot(receipt, keepUnresolved: true);
        }

        public NamedPipeWriteReceipt AppendStarted(string jobId)
            => Transition(jobId, NamedPipeWriteReceiptStates.Started, keepUnresolved: true);

        public NamedPipeWriteReceipt AppendCompleted(
            string jobId,
            string resultJson,
            long externalEventWaitMs,
            long addinExecutionToResultMs)
        {
            var bytes = Encoding.UTF8.GetByteCount(resultJson ?? string.Empty);
            if (bytes <= 0) throw new InvalidDataException("A completed receipt requires result JSON.");
            if (bytes > MaximumPersistedResultBytes)
                throw new InvalidDataException("Receipt result exceeds the 16 MiB persistence limit.");

            return Transition(
                jobId,
                NamedPipeWriteReceiptStates.Completed,
                keepUnresolved: true,
                update: receipt =>
                {
                    receipt.ResultJson = resultJson ?? string.Empty;
                    receipt.ResultSha256 = NamedPipeReceiptHash.ComputeUtf8Sha256(receipt.ResultJson);
                    receipt.ExternalEventWaitMs = Math.Max(0, externalEventWaitMs);
                    receipt.AddinExecutionToResultMs = Math.Max(0, addinExecutionToResultMs);
                });
        }

        public NamedPipeWriteReceipt AppendAcknowledged(string jobId, string code = "ACK")
            => Transition(jobId, NamedPipeWriteReceiptStates.Acknowledged, keepUnresolved: false,
                update: receipt => receipt.Code = code ?? "ACK");

        public NamedPipeWriteReceipt AppendRejected(string jobId, string code, string message)
            => Transition(jobId, NamedPipeWriteReceiptStates.Rejected, keepUnresolved: false,
                update: receipt =>
                {
                    receipt.Code = code ?? string.Empty;
                    receipt.Message = message ?? string.Empty;
                });

        public NamedPipeWriteReceipt AppendRecoveryRejected(string jobId, string code, string message)
            // Keep this unresolved. Dropping a rejected recovery could allow
            // the original durable job to be dispatched a second time.
            => Transition(jobId, NamedPipeWriteReceiptStates.RecoveryRejected, keepUnresolved: true,
                update: receipt =>
                {
                    receipt.Code = code ?? string.Empty;
                    receipt.Message = message ?? string.Empty;
                });

        private NamedPipeWriteReceipt Transition(
            string jobId,
            string state,
            bool keepUnresolved,
            Action<NamedPipeWriteReceipt>? update = null)
        {
            lock (_gate)
            {
                if (!_unresolved.TryGetValue(jobId ?? string.Empty, out var current))
                    throw new InvalidOperationException("Receipt was not found for jobId=" + (jobId ?? string.Empty) + ".");
                var next = current.Clone();
                next.State = state;
                update?.Invoke(next);
                return AppendSnapshotUnderLock(next, keepUnresolved);
            }
        }

        private NamedPipeWriteReceipt AppendSnapshot(NamedPipeWriteReceipt receipt, bool keepUnresolved)
        {
            lock (_gate) return AppendSnapshotUnderLock(receipt, keepUnresolved);
        }

        private NamedPipeWriteReceipt AppendSnapshotUnderLock(NamedPipeWriteReceipt receipt, bool keepUnresolved)
        {
            receipt.SchemaVersion = SchemaVersion;
            receipt.Sequence = _nextSequence++;
            receipt.UpdatedUtc = DateTimeOffset.UtcNow.ToString("o");

            var directory = Path.GetDirectoryName(FilePath);
            if (string.IsNullOrWhiteSpace(directory)) throw new InvalidOperationException("Receipt journal directory is invalid.");
            Directory.CreateDirectory(directory);
            var line = JsonSerializer.Serialize(receipt, _jsonOptions);
            var bytes = new UTF8Encoding(false).GetBytes(line + Environment.NewLine);
            using (var stream = new FileStream(FilePath, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                stream.Write(bytes, 0, bytes.Length);
                stream.Flush(true);
            }

            if (keepUnresolved) _unresolved[receipt.JobId] = receipt.Clone();
            else _unresolved.Remove(receipt.JobId);
            return receipt.Clone();
        }

        private void LoadExisting()
        {
            lock (_gate)
            {
                if (!File.Exists(FilePath)) return;
                foreach (var line in File.ReadLines(FilePath, Encoding.UTF8))
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    try
                    {
                        var receipt = JsonSerializer.Deserialize<NamedPipeWriteReceipt>(line, _jsonOptions);
                        if (receipt == null
                            || !string.Equals(receipt.SchemaVersion, SchemaVersion, StringComparison.Ordinal)
                            || string.IsNullOrWhiteSpace(receipt.JobId))
                        {
                            IgnoredCorruptLineCount++;
                            continue;
                        }

                        _nextSequence = Math.Max(_nextSequence, receipt.Sequence + 1);
                        if (IsUnresolvedState(receipt.State)) _unresolved[receipt.JobId] = receipt;
                        else _unresolved.Remove(receipt.JobId);
                    }
                    catch
                    {
                        IgnoredCorruptLineCount++;
                    }
                }
            }
        }

        private static bool IsUnresolvedState(string? state)
            => string.Equals(state, NamedPipeWriteReceiptStates.Accepted, StringComparison.Ordinal)
                || string.Equals(state, NamedPipeWriteReceiptStates.Started, StringComparison.Ordinal)
                || string.Equals(state, NamedPipeWriteReceiptStates.Completed, StringComparison.Ordinal)
                || string.Equals(state, NamedPipeWriteReceiptStates.RecoveryRejected, StringComparison.Ordinal);
    }
}

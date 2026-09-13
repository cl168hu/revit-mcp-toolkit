using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using RevitMCP.Abstractions.Bridge;
using RevitMcpServer.Bridge;

namespace RevitMcpServer.Engine
{
    public sealed class DurableQueue
    {
        private static readonly string[] TerminalStates = { "SUCCEEDED", "FAILED", "TIMEOUT", "DEAD" };
        private readonly JobDispatchSignal _dispatchSignal;
        private readonly JobCompletionSignal _completionSignal;

        public DurableQueue(
            JobDispatchSignal dispatchSignal,
            JobCompletionSignal? completionSignal = null)
        {
            _dispatchSignal = dispatchSignal ?? throw new ArgumentNullException(nameof(dispatchSignal));
            _completionSignal = completionSignal ?? new JobCompletionSignal();
        }

        public async Task<string> EnqueueAsync(string method, string paramsJson, string? idemKey, string? rpcId, int priority, int timeoutSec)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            if (!string.IsNullOrWhiteSpace(idemKey))
            {
                using var check = conn.CreateCommand();
                check.CommandText = "SELECT job_id FROM jobs WHERE idempotency_key=$k LIMIT 1";
                check.Parameters.AddWithValue("$k", idemKey);
                var exist = (string?)await check.ExecuteScalarAsync();
                if (!string.IsNullOrEmpty(exist)) return exist;
            }
            var jobId = Guid.NewGuid().ToString("N");
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"INSERT INTO jobs(job_id, method, params_json, rpc_id, idempotency_key, priority, state, enqueue_ts, timeout_sec, target_port)
VALUES($id,$m,$p,$rid,$k,$pri,'ENQUEUED',CURRENT_TIMESTAMP,$to,$tp);";
            cmd.Parameters.AddWithValue("$id", jobId);
            cmd.Parameters.AddWithValue("$m", method);
            cmd.Parameters.AddWithValue("$p", paramsJson);
            cmd.Parameters.AddWithValue("$rid", (object?)rpcId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$k", (object?)idemKey ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$pri", priority);
            cmd.Parameters.AddWithValue("$to", timeoutSec);
            try
            {
                var tp = RevitMcpServer.Infra.ServerContext.Port;
                if (tp <= 0 || tp >= 65536)
                {
                    var env = Environment.GetEnvironmentVariable("REVIT_MCP_PORT");
                    if (!int.TryParse(env, out tp)) tp = 0;
                }
                cmd.Parameters.AddWithValue("$tp", tp > 0 ? tp : (object)DBNull.Value);
            }
            catch { cmd.Parameters.AddWithValue("$tp", DBNull.Value); }
            await cmd.ExecuteNonQueryAsync();
            _dispatchSignal.Pulse();
            return jobId;
        }

        public async Task<dynamic?> GetAsync(string jobId)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM jobs WHERE job_id=$id";
            cmd.Parameters.AddWithValue("$id", jobId);
            using var r = await cmd.ExecuteReaderAsync();
            if (!await r.ReadAsync()) return null;
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < r.FieldCount; i++) row[r.GetName(i)] = r.GetValue(i);
            return row;
        }

        public async Task<IReadOnlyList<dynamic>> ListAsync(string state, int limit)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM jobs WHERE state=$s ORDER BY priority, enqueue_ts LIMIT $n";
            cmd.Parameters.AddWithValue("$s", state);
            cmd.Parameters.AddWithValue("$n", limit);
            var rows = new List<dynamic>();
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < r.FieldCount; i++) row[r.GetName(i)] = r.GetValue(i);
                rows.Add(row);
            }
            return rows;
        }

        public Task<dynamic?> ClaimAsync()
            => ClaimAsync(includeMethods: null, excludeMethods: null, jobPredicate: null);

        public Task<dynamic?> ClaimAsync(
            IReadOnlyCollection<string>? includeMethods,
            IReadOnlyCollection<string>? excludeMethods)
            => ClaimAsync(includeMethods, excludeMethods, jobPredicate: null);

        public async Task<dynamic?> ClaimAsync(
            IReadOnlyCollection<string>? includeMethods,
            IReadOnlyCollection<string>? excludeMethods,
            Func<string, string, bool>? jobPredicate)
        {
            var includes = NormalizeMethods(includeMethods);
            var excludes = NormalizeMethods(excludeMethods);
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var tx = conn.BeginTransaction();
            using (var pick = conn.CreateCommand())
            {
                pick.Transaction = tx;
                // Prefer matching jobs enqueued for this server port. As a fallback,
                // allow NULL/0 target_port for backward compatibility.
                var port = 0;
                try
                {
                    port = RevitMcpServer.Infra.ServerContext.Port;
                    if (port <= 0 || port >= 65536)
                    {
                        var env = Environment.GetEnvironmentVariable("REVIT_MCP_PORT");
                        if (!int.TryParse(env, out port)) port = 0;
                    }
                }
                catch { port = 0; }
                var methodClause = BuildMethodClause(pick, includes, excludes);
                if (port > 0)
                {
                    pick.CommandText = @"SELECT job_id, method, params_json FROM jobs WHERE state='ENQUEUED' AND (target_port=$p OR target_port IS NULL OR target_port=0)" + methodClause + " ORDER BY CASE WHEN target_port=$p THEN 0 ELSE 1 END, priority, enqueue_ts" + (jobPredicate == null ? " LIMIT 1" : string.Empty) + ";";
                    pick.Parameters.AddWithValue("$p", port);
                }
                else
                {
                    pick.CommandText = @"SELECT job_id, method, params_json FROM jobs WHERE state='ENQUEUED'" + methodClause + " ORDER BY priority, enqueue_ts" + (jobPredicate == null ? " LIMIT 1" : string.Empty) + ";";
                }

                string? jobId = null;
                using (var candidates = await pick.ExecuteReaderAsync())
                {
                    while (await candidates.ReadAsync())
                    {
                        var candidateId = candidates.IsDBNull(0) ? string.Empty : candidates.GetString(0);
                        var candidateMethod = candidates.IsDBNull(1) ? string.Empty : candidates.GetString(1);
                        var candidateParams = candidates.IsDBNull(2) ? "{}" : candidates.GetString(2);
                        if (jobPredicate != null && !jobPredicate(candidateMethod, candidateParams))
                            continue;

                        jobId = candidateId;
                        break;
                    }
                }
                if (string.IsNullOrEmpty(jobId)) { tx.Commit(); return null; }

                using var upd = conn.CreateCommand();
                upd.Transaction = tx;
                upd.CommandText = "UPDATE jobs SET state='DISPATCHING', start_ts=CURRENT_TIMESTAMP WHERE job_id=$id";
                upd.Parameters.AddWithValue("$id", jobId);
                await upd.ExecuteNonQueryAsync();

                using var get = conn.CreateCommand();
                get.Transaction = tx;
                get.CommandText = "SELECT * FROM jobs WHERE job_id=$id";
                get.Parameters.AddWithValue("$id", jobId);
                using var r = await get.ExecuteReaderAsync();
                await r.ReadAsync();
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < r.FieldCount; i++) row[r.GetName(i)] = r.GetValue(i);
                tx.Commit();
                return row;
            }
        }

        public async Task StartRunningAsync(string jobId)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE jobs SET state='RUNNING', heartbeat_ts=CURRENT_TIMESTAMP WHERE job_id=$id";
            cmd.Parameters.AddWithValue("$id", jobId);
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task HeartbeatAsync(string jobId)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE jobs SET heartbeat_ts=CURRENT_TIMESTAMP WHERE job_id=$id";
            cmd.Parameters.AddWithValue("$id", jobId);
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task CompleteAsync(string jobId, string resultJson)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"UPDATE jobs SET state='SUCCEEDED', finish_ts=CURRENT_TIMESTAMP, result_json=$r, error_code=NULL, error_msg=NULL WHERE job_id=$id";
            cmd.Parameters.AddWithValue("$id", jobId);
            cmd.Parameters.AddWithValue("$r", resultJson);
            var changed = await cmd.ExecuteNonQueryAsync();
            if (changed > 0) _completionSignal.Pulse();
        }

        public async Task<PipeReceiptRecoveryResult> ReconcileRecoveredResultAsync(
            string jobId,
            string method,
            string paramsSha256,
            string resultJson,
            string resultSha256)
        {
            if (string.IsNullOrWhiteSpace(jobId))
                return PipeReceiptRecoveryResult.Reject("RECOVERY_JOB_ID_REQUIRED", "jobId is required.");
            if (string.IsNullOrWhiteSpace(resultJson))
                return await RejectRecoveredJobAndStopReplayAsync(
                    jobId,
                    "RECOVERY_RESULT_REQUIRED",
                    "Recovered result JSON is required.").ConfigureAwait(false);

            try { using var ignored = JsonDocument.Parse(resultJson); }
            catch (Exception ex)
            {
                return await RejectRecoveredJobAndStopReplayAsync(
                    jobId,
                    "RECOVERY_RESULT_JSON_INVALID",
                    ex.Message).ConfigureAwait(false);
            }

            var computedResultHash = NamedPipeReceiptHash.ComputeUtf8Sha256(resultJson);
            if (!NamedPipeReceiptHash.FixedTimeEquals(computedResultHash, resultSha256))
                return await RejectRecoveredJobAndStopReplayAsync(
                    jobId,
                    "RECOVERY_RESULT_HASH_MISMATCH",
                    "Recovered result hash did not match its payload.").ConfigureAwait(false);

            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var read = conn.CreateCommand();
            read.CommandText = "SELECT method, params_json, state, result_json, error_code FROM jobs WHERE job_id=$id";
            read.Parameters.AddWithValue("$id", jobId);
            using var reader = await read.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return PipeReceiptRecoveryResult.Reject("RECOVERY_JOB_NOT_FOUND", "The receipt job does not exist in the durable queue.");

            var storedMethod = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            var storedParams = reader.IsDBNull(1) ? "{}" : reader.GetString(1);
            var state = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
            var storedResult = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
            var errorCode = reader.IsDBNull(4) ? string.Empty : reader.GetString(4);
            await reader.DisposeAsync();

            var identityFailure = ValidateRecoveredIdentity(storedMethod, storedParams, method, paramsSha256);
            if (identityFailure != null)
            {
                return await RejectRecoveredJobAndStopReplayAsync(
                    jobId,
                    identityFailure.Code,
                    identityFailure.Message,
                    state).ConfigureAwait(false);
            }

            if (string.Equals(state, "SUCCEEDED", StringComparison.OrdinalIgnoreCase))
            {
                var storedHash = NamedPipeReceiptHash.ComputeUtf8Sha256(storedResult);
                return NamedPipeReceiptHash.FixedTimeEquals(storedHash, computedResultHash)
                    ? PipeReceiptRecoveryResult.Accept("RECOVERY_ALREADY_SUCCEEDED", "The durable job already contains the same result.", state)
                    : PipeReceiptRecoveryResult.Reject("RECOVERY_SUCCEEDED_RESULT_CONFLICT", "The durable job already succeeded with a different result.", state);
            }

            var recoverable = string.Equals(state, "ENQUEUED", StringComparison.OrdinalIgnoreCase)
                || string.Equals(state, "DISPATCHING", StringComparison.OrdinalIgnoreCase)
                || string.Equals(state, "RUNNING", StringComparison.OrdinalIgnoreCase)
                || (string.Equals(state, "DEAD", StringComparison.OrdinalIgnoreCase)
                    && (string.Equals(errorCode, "PIPE_DRY_RUN_OUTCOME_UNKNOWN", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(errorCode, "STALE", StringComparison.OrdinalIgnoreCase)));
            if (!recoverable)
                return PipeReceiptRecoveryResult.Reject("RECOVERY_STATE_CONFLICT", "The durable job state cannot accept a recovered result.", state);

            using var update = conn.CreateCommand();
            update.CommandText = @"UPDATE jobs
SET state='SUCCEEDED', finish_ts=CURRENT_TIMESTAMP, result_json=$result, error_code=NULL, error_msg=NULL
WHERE job_id=$id AND state=$expectedState";
            update.Parameters.AddWithValue("$result", resultJson);
            update.Parameters.AddWithValue("$id", jobId);
            update.Parameters.AddWithValue("$expectedState", state);
            var changed = await update.ExecuteNonQueryAsync();
            if (changed == 1) _completionSignal.Pulse();
            return changed == 1
                ? PipeReceiptRecoveryResult.Accept("RECOVERY_RESULT_APPLIED", "The persisted Add-in result was reconciled without Revit re-execution.", "SUCCEEDED")
                : PipeReceiptRecoveryResult.Reject("RECOVERY_STATE_RACE", "The durable job changed while receipt recovery was being applied.", state);
        }

        public async Task<PipeReceiptRecoveryResult> ConfirmRecoveredUnknownAsync(
            string jobId,
            string method,
            string paramsSha256,
            string receiptState)
        {
            if (string.IsNullOrWhiteSpace(jobId))
                return PipeReceiptRecoveryResult.Reject("RECOVERY_JOB_ID_REQUIRED", "jobId is required.");

            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var read = conn.CreateCommand();
            read.CommandText = "SELECT method, params_json, state, error_code FROM jobs WHERE job_id=$id";
            read.Parameters.AddWithValue("$id", jobId);
            using var reader = await read.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return PipeReceiptRecoveryResult.Reject("RECOVERY_JOB_NOT_FOUND", "The receipt job does not exist in the durable queue.");

            var storedMethod = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            var storedParams = reader.IsDBNull(1) ? "{}" : reader.GetString(1);
            var state = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
            var errorCode = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
            await reader.DisposeAsync();

            var identityFailure = ValidateRecoveredIdentity(storedMethod, storedParams, method, paramsSha256);
            if (identityFailure != null)
            {
                return await RejectRecoveredJobAndStopReplayAsync(
                    jobId,
                    identityFailure.Code,
                    identityFailure.Message,
                    state).ConfigureAwait(false);
            }

            if (string.Equals(state, "SUCCEEDED", StringComparison.OrdinalIgnoreCase))
                return PipeReceiptRecoveryResult.Accept("RECOVERY_UNKNOWN_ALREADY_SUCCEEDED", "The Server already has a completed result.", state);

            if (string.Equals(state, "DEAD", StringComparison.OrdinalIgnoreCase)
                && string.Equals(errorCode, "PIPE_DRY_RUN_OUTCOME_UNKNOWN", StringComparison.OrdinalIgnoreCase))
            {
                return PipeReceiptRecoveryResult.Accept("RECOVERY_UNKNOWN_CONFIRMED", "The unknown outcome remains terminal and will not be retried.", state);
            }

            var mayBeInFlight = string.Equals(state, "ENQUEUED", StringComparison.OrdinalIgnoreCase)
                || string.Equals(state, "DISPATCHING", StringComparison.OrdinalIgnoreCase)
                || string.Equals(state, "RUNNING", StringComparison.OrdinalIgnoreCase);
            if (!mayBeInFlight)
                return PipeReceiptRecoveryResult.Reject("RECOVERY_STATE_CONFLICT", "The durable job state conflicts with the unresolved Add-in receipt.", state);

            using var update = conn.CreateCommand();
            update.CommandText = @"UPDATE jobs
SET state='DEAD', finish_ts=CURRENT_TIMESTAMP, error_code='PIPE_DRY_RUN_OUTCOME_UNKNOWN', error_msg=$message
WHERE job_id=$id AND state=$expectedState";
            update.Parameters.AddWithValue("$message", "Persistent Add-in receipt stopped at state '" + (receiptState ?? string.Empty) + "'. Automatic retry is disabled.");
            update.Parameters.AddWithValue("$id", jobId);
            update.Parameters.AddWithValue("$expectedState", state);
            var changed = await update.ExecuteNonQueryAsync();
            if (changed == 1) _completionSignal.Pulse();
            return changed == 1
                ? PipeReceiptRecoveryResult.Accept("RECOVERY_UNKNOWN_MARKED_DEAD", "The unresolved receipt was made terminal without replay.", "DEAD")
                : PipeReceiptRecoveryResult.Reject("RECOVERY_STATE_RACE", "The durable job changed while unknown-outcome recovery was being applied.", state);
        }

        private static PipeReceiptRecoveryResult? ValidateRecoveredIdentity(
            string storedMethod,
            string storedParams,
            string receiptMethod,
            string receiptParamsSha256)
        {
            if (!string.Equals(storedMethod, receiptMethod, StringComparison.Ordinal))
                return PipeReceiptRecoveryResult.Reject("RECOVERY_METHOD_MISMATCH", "Receipt method does not match the durable job.");
            if (!NamedPipeCommandPolicy.IsExplicitDryRunWrite(storedMethod, storedParams))
                return PipeReceiptRecoveryResult.Reject("RECOVERY_COMMAND_POLICY_REJECTED", "Only an explicitly allowed dryRun:true write may be recovered.");

            var storedParamsHash = NamedPipeReceiptHash.ComputeCanonicalJsonSha256(storedParams);
            return NamedPipeReceiptHash.FixedTimeEquals(storedParamsHash, receiptParamsSha256)
                ? null
                : PipeReceiptRecoveryResult.Reject("RECOVERY_PARAMS_HASH_MISMATCH", "Receipt params hash does not match the durable job.");
        }

        private async Task<PipeReceiptRecoveryResult> RejectRecoveredJobAndStopReplayAsync(
            string jobId,
            string code,
            string message,
            string state = "")
        {
            var finalState = state ?? string.Empty;
            if (!string.IsNullOrWhiteSpace(jobId))
            {
                using var conn = Persistence.SqliteConnectionFactory.Create();
                using var update = conn.CreateCommand();
                update.CommandText = @"UPDATE jobs
SET state='DEAD', finish_ts=CURRENT_TIMESTAMP,
    error_code='PIPE_RECEIPT_RECOVERY_REJECTED', error_msg=$message
WHERE job_id=$id AND state IN ('ENQUEUED','DISPATCHING','RUNNING')";
                update.Parameters.AddWithValue("$id", jobId);
                update.Parameters.AddWithValue("$message", code + ": " + (message ?? string.Empty)
                    + " Automatic replay is disabled.");
                if (await update.ExecuteNonQueryAsync().ConfigureAwait(false) == 1)
                {
                    finalState = "DEAD";
                    _completionSignal.Pulse();
                }
            }

            return PipeReceiptRecoveryResult.Reject(code, message ?? string.Empty, finalState);
        }

        public async Task FailAsync(string jobId, string code, string msg)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"UPDATE jobs SET state='FAILED', finish_ts=CURRENT_TIMESTAMP, error_code=$c, error_msg=$m, attempts=attempts+1 WHERE job_id=$id";
            cmd.Parameters.AddWithValue("$id", jobId);
            cmd.Parameters.AddWithValue("$c", code);
            cmd.Parameters.AddWithValue("$m", msg);
            var changed = await cmd.ExecuteNonQueryAsync();
            if (changed > 0) _completionSignal.Pulse();
        }

        public async Task TimeoutAsync(string jobId)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"UPDATE jobs SET state='TIMEOUT', finish_ts=CURRENT_TIMESTAMP, error_code='TIMEOUT', error_msg='heartbeat lost', attempts=attempts+1 WHERE job_id=$id";
            cmd.Parameters.AddWithValue("$id", jobId);
            var changed = await cmd.ExecuteNonQueryAsync();
            if (changed > 0) _completionSignal.Pulse();
        }

        public async Task RequeueAsync(string jobId)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE jobs SET state='ENQUEUED', start_ts=NULL, heartbeat_ts=NULL, finish_ts=NULL WHERE job_id=$id";
            cmd.Parameters.AddWithValue("$id", jobId);
            await cmd.ExecuteNonQueryAsync();
            _dispatchSignal.Pulse();
        }

        private static List<string> NormalizeMethods(IReadOnlyCollection<string>? methods)
        {
            if (methods == null || methods.Count == 0) return new List<string>();
            return methods
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => x.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private static string BuildMethodClause(
            SqliteCommand command,
            IReadOnlyList<string> includes,
            IReadOnlyList<string> excludes)
        {
            if (includes.Count > 0)
            {
                var names = new List<string>(includes.Count);
                for (var i = 0; i < includes.Count; i++)
                {
                    var name = "$includeMethod" + i.ToString(CultureInfo.InvariantCulture);
                    names.Add(name);
                    command.Parameters.AddWithValue(name, includes[i]);
                }
                return " AND method COLLATE NOCASE IN (" + string.Join(",", names) + ")";
            }

            if (excludes.Count > 0)
            {
                var names = new List<string>(excludes.Count);
                for (var i = 0; i < excludes.Count; i++)
                {
                    var name = "$excludeMethod" + i.ToString(CultureInfo.InvariantCulture);
                    names.Add(name);
                    command.Parameters.AddWithValue(name, excludes[i]);
                }
                return " AND method COLLATE NOCASE NOT IN (" + string.Join(",", names) + ")";
            }

            return string.Empty;
        }

        public async Task DeadAsync(string jobId, string code, string msg)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"UPDATE jobs SET state='DEAD', finish_ts=CURRENT_TIMESTAMP, error_code=$c, error_msg=$m WHERE job_id=$id";
            cmd.Parameters.AddWithValue("$id", jobId);
            cmd.Parameters.AddWithValue("$c", code);
            cmd.Parameters.AddWithValue("$m", msg);
            var changed = await cmd.ExecuteNonQueryAsync();
            if (changed > 0) _completionSignal.Pulse();
        }

        public async Task<string?> FindRunningJobIdByRpcIdAsync(string rpcId)
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"SELECT job_id FROM jobs WHERE rpc_id=$rid AND state IN ('RUNNING','DISPATCHING') ORDER BY start_ts DESC LIMIT 1";
            cmd.Parameters.AddWithValue("$rid", rpcId);
            var obj = await cmd.ExecuteScalarAsync();
            return obj as string;
        }

        // ----------------------------- Step 10: status helpers -----------------------------

        /// <summary>
        /// Reclaim "in progress" jobs (RUNNING/DISPATCHING) that are very likely orphaned.
        /// Uses a conservative "staleAfterSeconds" threshold to avoid impacting long-running commands.
        /// </summary>
        public async Task<int> ReclaimStaleInProgressJobsAsync(int staleAfterSeconds)
        {
            if (staleAfterSeconds <= 0) return 0;

            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();

            var port = ResolveCurrentPort();
            if (port > 0)
            {
                cmd.CommandText =
                    @"UPDATE jobs
SET state='DEAD',
    finish_ts=CURRENT_TIMESTAMP,
    error_code='STALE',
    error_msg='stale in-progress job reclaimed'
WHERE state IN ('RUNNING','DISPATCHING')
  AND (target_port=$p OR target_port IS NULL OR target_port=0)
  AND (strftime('%s','now') - strftime('%s', COALESCE(heartbeat_ts, start_ts, enqueue_ts))) > $stale;";
                cmd.Parameters.AddWithValue("$p", port);
            }
            else
            {
                cmd.CommandText =
                    @"UPDATE jobs
SET state='DEAD',
    finish_ts=CURRENT_TIMESTAMP,
    error_code='STALE',
    error_msg='stale in-progress job reclaimed'
WHERE state IN ('RUNNING','DISPATCHING')
  AND (strftime('%s','now') - strftime('%s', COALESCE(heartbeat_ts, start_ts, enqueue_ts))) > $stale;";
            }

            cmd.Parameters.AddWithValue("$stale", staleAfterSeconds);
            var changed = await cmd.ExecuteNonQueryAsync();
            if (changed > 0) _completionSignal.Pulse();
            return changed;
        }

        public async Task<IDictionary<string, long>> CountByStateAsync()
        {
            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();

            var port = ResolveCurrentPort();
            if (port > 0)
            {
                cmd.CommandText = "SELECT state, COUNT(*) AS cnt FROM jobs WHERE (target_port=$p OR target_port IS NULL OR target_port=0) GROUP BY state";
                cmd.Parameters.AddWithValue("$p", port);
            }
            else
            {
                cmd.CommandText = "SELECT state, COUNT(*) AS cnt FROM jobs GROUP BY state";
            }

            var dict = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                var state = r.GetString(0);
                var cnt = r.GetInt64(1);
                dict[state] = cnt;
            }
            return dict;
        }

        public async Task<dynamic?> GetLatestJobByStatesAsync(string[] states, string orderByColumnDesc)
        {
            if (states == null || states.Length == 0) return null;

            using var conn = Persistence.SqliteConnectionFactory.Create();
            using var cmd = conn.CreateCommand();

            // States are constants from the server; inline them safely.
            var inList = string.Join(",", states.Select(s => "'" + s.Replace("'", "''") + "'"));

            var port = ResolveCurrentPort();
            if (port > 0)
            {
                cmd.CommandText = $"SELECT * FROM jobs WHERE state IN ({inList}) AND (target_port=$p OR target_port IS NULL OR target_port=0) ORDER BY {orderByColumnDesc} DESC LIMIT 1";
                cmd.Parameters.AddWithValue("$p", port);
            }
            else
            {
                cmd.CommandText = $"SELECT * FROM jobs WHERE state IN ({inList}) ORDER BY {orderByColumnDesc} DESC LIMIT 1";
            }

            using var r = await cmd.ExecuteReaderAsync();
            if (!await r.ReadAsync()) return null;

            var row = new Dictionary<string, object?>();
            for (int i = 0; i < r.FieldCount; i++) row[r.GetName(i)] = r.GetValue(i);
            return row;
        }

        /// <summary>
        /// Deletes terminal jobs only. Active and queued jobs are never touched.
        /// Work is bounded so maintenance does not monopolize SQLite on a busy Revit session.
        /// </summary>
        public async Task<JobRetentionReport> PruneTerminalJobsAsync(
            int maxAgeDays,
            int maxCompletedJobs,
            long maxTerminalPayloadBytes,
            long maxDatabaseBytes,
            int minimumTerminalAgeMinutes,
            int deleteBatchSize,
            int maxDeleteBatches,
            int incrementalVacuumPages,
            CancellationToken cancellationToken)
        {
            var report = new JobRetentionReport();
            deleteBatchSize = Math.Max(100, Math.Min(10000, deleteBatchSize));
            maxDeleteBatches = Math.Max(1, Math.Min(100, maxDeleteBatches));
            minimumTerminalAgeMinutes = Math.Max(5, Math.Min(10080, minimumTerminalAgeMinutes));
            incrementalVacuumPages = Math.Max(256, Math.Min(131072, incrementalVacuumPages));
            var remainingBudget = deleteBatchSize * maxDeleteBatches;

            using var conn = Persistence.SqliteConnectionFactory.Create();
            var before = await ReadQueueStorageStatsAsync(conn, cancellationToken);
            report.DatabaseBytesBefore = before.DatabaseBytes;
            report.WalBytesBefore = before.WalBytes;
            report.FreelistBytesBefore = before.FreelistBytes;
            report.AutoVacuumModeBefore = before.AutoVacuumMode;
            report.ActiveJobsAtStart = await CountActiveJobsAsync(conn, cancellationToken);
            report.TerminalPayloadBytesBefore = await SumTerminalPayloadBytesAsync(conn, cancellationToken);
            var protectedCutoff = DateTime.UtcNow.AddMinutes(-minimumTerminalAgeMinutes)
                .ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

            if (maxAgeDays > 0 && remainingBudget > 0)
            {
                var cutoff = DateTime.UtcNow.AddDays(-maxAgeDays).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                while (remainingBudget > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var take = Math.Min(deleteBatchSize, remainingBudget);
                    var deleted = await DeleteOldestTerminalJobsAsync(conn, cutoff, take, cancellationToken);
                    report.DeletedByAge += deleted;
                    remainingBudget -= deleted;
                    if (deleted < take) break;
                }
            }

            report.TerminalCountBeforeCountLimit = await CountTerminalJobsAsync(conn, cancellationToken);
            if (maxCompletedJobs > 0 && report.TerminalCountBeforeCountLimit > maxCompletedJobs && remainingBudget > 0)
            {
                var excess = report.TerminalCountBeforeCountLimit - maxCompletedJobs;
                while (excess > 0 && remainingBudget > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var take = (int)Math.Min(Math.Min(excess, deleteBatchSize), remainingBudget);
                    var deleted = await DeleteOldestTerminalJobsAsync(conn, protectedCutoff, take, cancellationToken);
                    report.DeletedByCount += deleted;
                    remainingBudget -= deleted;
                    excess -= deleted;
                    if (deleted < take) break;
                }
            }

            report.TerminalPayloadBytesBeforePayloadLimit = await SumTerminalPayloadBytesAsync(conn, cancellationToken);
            if (maxTerminalPayloadBytes > 0 &&
                report.TerminalPayloadBytesBeforePayloadLimit > maxTerminalPayloadBytes &&
                remainingBudget > 0)
            {
                var excessBytes = report.TerminalPayloadBytesBeforePayloadLimit - maxTerminalPayloadBytes;
                while (excessBytes > 0 && remainingBudget > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var take = Math.Min(deleteBatchSize, remainingBudget);
                    var deleted = await DeleteOldestTerminalPayloadAsync(
                        conn,
                        protectedCutoff,
                        excessBytes,
                        take,
                        cancellationToken);
                    report.DeletedByPayload += deleted.DeletedCount;
                    remainingBudget -= deleted.DeletedCount;
                    if (deleted.DeletedCount == 0) break;
                    excessBytes = Math.Max(0L, excessBytes - deleted.DeletedPayloadBytes);
                }
            }

            report.RemainingTerminalJobs = await CountTerminalJobsAsync(conn, cancellationToken);
            report.TerminalPayloadBytesAfter = await SumTerminalPayloadBytesAsync(conn, cancellationToken);
            var eligibleTerminalJobs = await CountEligibleTerminalJobsAsync(conn, protectedCutoff, cancellationToken);
            var countOverLimit = maxCompletedJobs > 0 && report.RemainingTerminalJobs > maxCompletedJobs;
            var payloadOverLimit = maxTerminalPayloadBytes > 0 && report.TerminalPayloadBytesAfter > maxTerminalPayloadBytes;
            report.RetentionDeferredByRecentJobs = (countOverLimit || payloadOverLimit) && eligibleTerminalJobs == 0;
            report.HasMore = (countOverLimit || payloadOverLimit) && eligibleTerminalJobs > 0;

            if (report.TotalDeleted > 0)
            {
                try
                {
                    using var checkpoint = conn.CreateCommand();
                    checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize;";
                    await checkpoint.ExecuteNonQueryAsync(cancellationToken);
                }
                catch { /* best-effort; retention rows are already committed */ }
            }

            var middle = await ReadQueueStorageStatsAsync(conn, cancellationToken);
            report.IncrementalVacuumPagesRequested = incrementalVacuumPages;
            if (middle.AutoVacuumMode == 2 && middle.FreelistPages > 0 && report.ActiveJobsAtStart == 0)
            {
                try
                {
                    using var vacuum = conn.CreateCommand();
                    vacuum.CommandText = "PRAGMA incremental_vacuum(" + incrementalVacuumPages.ToString(CultureInfo.InvariantCulture) + ");";
                    await vacuum.ExecuteNonQueryAsync(cancellationToken);
                    report.IncrementalVacuumAttempted = true;
                    using var checkpoint = conn.CreateCommand();
                    checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
                    await checkpoint.ExecuteNonQueryAsync(cancellationToken);
                }
                catch
                {
                    report.IncrementalVacuumFailed = true;
                }
            }

            var after = await ReadQueueStorageStatsAsync(conn, cancellationToken);
            report.DatabaseBytesAfter = after.DatabaseBytes;
            report.WalBytesAfter = after.WalBytes;
            report.FreelistBytesAfter = after.FreelistBytes;
            report.AutoVacuumModeAfter = after.AutoVacuumMode;
            report.DatabaseOverLimitAfter = maxDatabaseBytes > 0 && report.DatabaseBytesAfter > maxDatabaseBytes;
            report.OfflineCompactionRequired = report.DatabaseOverLimitAfter &&
                report.AutoVacuumModeAfter != 2 &&
                report.FreelistBytesAfter > 0;
            if (report.DatabaseOverLimitAfter &&
                report.AutoVacuumModeAfter == 2 &&
                report.FreelistBytesAfter > 0 &&
                report.ActiveJobsAtStart == 0)
            {
                report.HasMore = true;
            }
            return report;
        }

        private static async Task<int> DeleteOldestTerminalJobsAsync(
            SqliteConnection conn,
            string olderThanUtc,
            int take,
            CancellationToken cancellationToken)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"DELETE FROM jobs
WHERE job_id IN (
    SELECT job_id
    FROM jobs
    WHERE state IN ('SUCCEEDED','FAILED','TIMEOUT','DEAD')
      AND COALESCE(finish_ts, enqueue_ts) < $cutoff
    ORDER BY COALESCE(finish_ts, enqueue_ts), rowid
    LIMIT $take
);";
            cmd.Parameters.AddWithValue("$cutoff", olderThanUtc);
            cmd.Parameters.AddWithValue("$take", take);
            return await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private static async Task<PayloadDeleteResult> DeleteOldestTerminalPayloadAsync(
            SqliteConnection conn,
            string olderThanUtc,
            long bytesToFree,
            int take,
            CancellationToken cancellationToken)
        {
            var selected = new List<(string JobId, long PayloadBytes)>();
            using (var pick = conn.CreateCommand())
            {
                pick.CommandText = @"SELECT job_id,
       COALESCE(length(params_json),0) + COALESCE(length(result_json),0) +
       COALESCE(length(error_code),0) + COALESCE(length(error_msg),0) AS payload_bytes
FROM jobs
WHERE state IN ('SUCCEEDED','FAILED','TIMEOUT','DEAD')
  AND COALESCE(finish_ts, enqueue_ts) < $cutoff
ORDER BY COALESCE(finish_ts, enqueue_ts), rowid
LIMIT $take;";
                pick.Parameters.AddWithValue("$cutoff", olderThanUtc);
                pick.Parameters.AddWithValue("$take", take);
                using var reader = await pick.ExecuteReaderAsync(cancellationToken);
                long selectedBytes = 0L;
                while (await reader.ReadAsync(cancellationToken))
                {
                    var row = (reader.GetString(0), reader.IsDBNull(1) ? 0L : reader.GetInt64(1));
                    selected.Add(row);
                    selectedBytes += row.Item2;
                    if (selectedBytes >= bytesToFree) break;
                }
            }
            if (selected.Count == 0) return new PayloadDeleteResult(0, 0L);

            using var delete = conn.CreateCommand();
            var names = new List<string>(selected.Count);
            for (var i = 0; i < selected.Count; i++)
            {
                var name = "$id" + i.ToString(CultureInfo.InvariantCulture);
                names.Add(name);
                delete.Parameters.AddWithValue(name, selected[i].JobId);
            }
            delete.CommandText = "DELETE FROM jobs WHERE job_id IN (" + string.Join(",", names) + ");";
            var count = await delete.ExecuteNonQueryAsync(cancellationToken);
            return new PayloadDeleteResult(count, selected.Sum(x => x.PayloadBytes));
        }

        private static async Task<long> CountTerminalJobsAsync(SqliteConnection conn, CancellationToken cancellationToken)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM jobs WHERE state IN ('SUCCEEDED','FAILED','TIMEOUT','DEAD');";
            var value = await cmd.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt64(value ?? 0, CultureInfo.InvariantCulture);
        }

        private static async Task<long> CountEligibleTerminalJobsAsync(
            SqliteConnection conn,
            string olderThanUtc,
            CancellationToken cancellationToken)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"SELECT COUNT(*) FROM jobs
WHERE state IN ('SUCCEEDED','FAILED','TIMEOUT','DEAD')
  AND COALESCE(finish_ts, enqueue_ts) < $cutoff;";
            cmd.Parameters.AddWithValue("$cutoff", olderThanUtc);
            var value = await cmd.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt64(value ?? 0, CultureInfo.InvariantCulture);
        }

        private static async Task<long> CountActiveJobsAsync(SqliteConnection conn, CancellationToken cancellationToken)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM jobs WHERE state IN ('ENQUEUED','DISPATCHING','RUNNING');";
            var value = await cmd.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt64(value ?? 0, CultureInfo.InvariantCulture);
        }

        private static async Task<long> SumTerminalPayloadBytesAsync(SqliteConnection conn, CancellationToken cancellationToken)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"SELECT COALESCE(SUM(
    COALESCE(length(params_json),0) + COALESCE(length(result_json),0) +
    COALESCE(length(error_code),0) + COALESCE(length(error_msg),0)),0)
FROM jobs WHERE state IN ('SUCCEEDED','FAILED','TIMEOUT','DEAD');";
            var value = await cmd.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt64(value ?? 0, CultureInfo.InvariantCulture);
        }

        private static async Task<QueueStorageStats> ReadQueueStorageStatsAsync(
            SqliteConnection conn,
            CancellationToken cancellationToken)
        {
            var pageCount = await ReadPragmaLongAsync(conn, "page_count", cancellationToken);
            var pageSize = await ReadPragmaLongAsync(conn, "page_size", cancellationToken);
            var freelistPages = await ReadPragmaLongAsync(conn, "freelist_count", cancellationToken);
            var autoVacuumMode = (int)await ReadPragmaLongAsync(conn, "auto_vacuum", cancellationToken);
            var databasePath = conn.DataSource ?? string.Empty;
            long databaseBytes = 0L;
            long walBytes = 0L;
            try
            {
                if (File.Exists(databasePath)) databaseBytes = new FileInfo(databasePath).Length;
                var walPath = databasePath + "-wal";
                if (File.Exists(walPath)) walBytes = new FileInfo(walPath).Length;
            }
            catch { }
            return new QueueStorageStats(
                databaseBytes,
                walBytes,
                freelistPages,
                freelistPages * pageSize,
                pageCount * pageSize,
                autoVacuumMode);
        }

        private static async Task<long> ReadPragmaLongAsync(
            SqliteConnection conn,
            string pragmaName,
            CancellationToken cancellationToken)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "PRAGMA " + pragmaName + ";";
            var value = await cmd.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt64(value ?? 0, CultureInfo.InvariantCulture);
        }

        public int ResolveCurrentPort()
        {
            try
            {
                var port = RevitMcpServer.Infra.ServerContext.Port;
                if (port > 0 && port < 65536) return port;

                var env = Environment.GetEnvironmentVariable("REVIT_MCP_PORT");
                if (int.TryParse(env, out port) && port > 0 && port < 65536) return port;
            }
            catch { /* ignore */ }
            return 0;
        }
    }

    public sealed class JobRetentionReport
    {
        public int DeletedByAge { get; set; }
        public int DeletedByCount { get; set; }
        public int DeletedByPayload { get; set; }
        public long TerminalCountBeforeCountLimit { get; set; }
        public long RemainingTerminalJobs { get; set; }
        public long TerminalPayloadBytesBefore { get; set; }
        public long TerminalPayloadBytesBeforePayloadLimit { get; set; }
        public long TerminalPayloadBytesAfter { get; set; }
        public long ActiveJobsAtStart { get; set; }
        public long DatabaseBytesBefore { get; set; }
        public long DatabaseBytesAfter { get; set; }
        public long WalBytesBefore { get; set; }
        public long WalBytesAfter { get; set; }
        public long FreelistBytesBefore { get; set; }
        public long FreelistBytesAfter { get; set; }
        public int AutoVacuumModeBefore { get; set; }
        public int AutoVacuumModeAfter { get; set; }
        public int IncrementalVacuumPagesRequested { get; set; }
        public bool IncrementalVacuumAttempted { get; set; }
        public bool IncrementalVacuumFailed { get; set; }
        public bool RetentionDeferredByRecentJobs { get; set; }
        public bool DatabaseOverLimitAfter { get; set; }
        public bool OfflineCompactionRequired { get; set; }
        public bool HasMore { get; set; }
        public int TotalDeleted => DeletedByAge + DeletedByCount + DeletedByPayload;
    }

    internal readonly struct PayloadDeleteResult
    {
        public PayloadDeleteResult(int deletedCount, long deletedPayloadBytes)
        {
            DeletedCount = deletedCount;
            DeletedPayloadBytes = deletedPayloadBytes;
        }

        public int DeletedCount { get; }
        public long DeletedPayloadBytes { get; }
    }

    internal readonly struct QueueStorageStats
    {
        public QueueStorageStats(
            long databaseBytes,
            long walBytes,
            long freelistPages,
            long freelistBytes,
            long logicalBytes,
            int autoVacuumMode)
        {
            DatabaseBytes = databaseBytes;
            WalBytes = walBytes;
            FreelistPages = freelistPages;
            FreelistBytes = freelistBytes;
            LogicalBytes = logicalBytes;
            AutoVacuumMode = autoVacuumMode;
        }

        public long DatabaseBytes { get; }
        public long WalBytes { get; }
        public long FreelistPages { get; }
        public long FreelistBytes { get; }
        public long LogicalBytes { get; }
        public int AutoVacuumMode { get; }
    }

    public sealed class PipeReceiptRecoveryResult
    {
        private PipeReceiptRecoveryResult(bool ok, string code, string message, string state)
        {
            Ok = ok;
            Code = code ?? string.Empty;
            Message = message ?? string.Empty;
            State = state ?? string.Empty;
        }

        public bool Ok { get; }
        public string Code { get; }
        public string Message { get; }
        public string State { get; }

        public static PipeReceiptRecoveryResult Accept(string code, string message, string state)
            => new PipeReceiptRecoveryResult(true, code, message, state);

        public static PipeReceiptRecoveryResult Reject(string code, string message, string state = "")
            => new PipeReceiptRecoveryResult(false, code, message, state);
    }
}

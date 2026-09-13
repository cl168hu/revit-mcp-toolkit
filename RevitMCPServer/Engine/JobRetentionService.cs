using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using RevitMcpServer.Infra;

namespace RevitMcpServer.Engine
{
    /// <summary>
    /// Bounded background cleanup for the durable RPC queue.
    /// It never deletes ENQUEUED, DISPATCHING, or RUNNING jobs.
    /// </summary>
    internal sealed class JobRetentionService : BackgroundService
    {
        private readonly DurableQueue _queue;
        private readonly bool _enabled;
        private readonly int _maxAgeDays;
        private readonly int _maxCompletedJobs;
        private readonly long _maxTerminalPayloadBytes;
        private readonly long _maxDatabaseBytes;
        private readonly int _minimumTerminalAgeMinutes;
        private readonly int _incrementalVacuumPages;
        private readonly int _intervalMinutes;
        private readonly int _deleteBatchSize;
        private readonly int _maxDeleteBatches;
        private bool _offlineCompactionWarningLogged;

        public JobRetentionService(DurableQueue queue, IConfiguration configuration)
        {
            _queue = queue;
            _enabled = ReadBool(configuration, "JobRetention:Enabled", "REVIT_MCP_JOB_RETENTION_ENABLED", true);
            _maxAgeDays = ReadInt(configuration, "JobRetention:MaxAgeDays", "REVIT_MCP_JOB_RETENTION_DAYS", 7, 1, 3650);
            _maxCompletedJobs = ReadInt(configuration, "JobRetention:MaxCompletedJobs", "REVIT_MCP_JOB_RETENTION_MAX_COMPLETED", 2000, 100, 1000000);
            _maxTerminalPayloadBytes = ReadLong(configuration, "JobRetention:MaxTerminalPayloadMiB", "REVIT_MCP_JOB_RETENTION_MAX_PAYLOAD_MIB", 128, 16, 16384) * 1024L * 1024L;
            _maxDatabaseBytes = ReadLong(configuration, "JobRetention:MaxDatabaseMiB", "REVIT_MCP_JOB_RETENTION_MAX_DATABASE_MIB", 512, 64, 65536) * 1024L * 1024L;
            _minimumTerminalAgeMinutes = ReadInt(configuration, "JobRetention:MinimumTerminalAgeMinutes", "REVIT_MCP_JOB_RETENTION_MIN_AGE_MINUTES", 60, 5, 10080);
            _incrementalVacuumPages = ReadInt(configuration, "JobRetention:IncrementalVacuumPagesPerPass", "REVIT_MCP_JOB_RETENTION_VACUUM_PAGES", 16384, 256, 131072);
            _intervalMinutes = ReadInt(configuration, "JobRetention:CleanupIntervalMinutes", "REVIT_MCP_JOB_RETENTION_INTERVAL_MINUTES", 5, 1, 1440);
            _deleteBatchSize = ReadInt(configuration, "JobRetention:DeleteBatchSize", "REVIT_MCP_JOB_RETENTION_BATCH_SIZE", 500, 100, 10000);
            _maxDeleteBatches = ReadInt(configuration, "JobRetention:MaxDeleteBatchesPerPass", "REVIT_MCP_JOB_RETENTION_MAX_BATCHES", 20, 1, 100);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!_enabled) return;

            try { await Task.Delay(TimeSpan.FromSeconds(20), stoppingToken); }
            catch (OperationCanceledException) { return; }

            while (!stoppingToken.IsCancellationRequested)
            {
                var runSoon = false;
                try
                {
                    var report = await _queue.PruneTerminalJobsAsync(
                        _maxAgeDays,
                        _maxCompletedJobs,
                        _maxTerminalPayloadBytes,
                        _maxDatabaseBytes,
                        _minimumTerminalAgeMinutes,
                        _deleteBatchSize,
                        _maxDeleteBatches,
                        _incrementalVacuumPages,
                        stoppingToken);
                    runSoon = report.HasMore;
                    var firstOfflineWarning = report.OfflineCompactionRequired && !_offlineCompactionWarningLogged;
                    if (report.TotalDeleted > 0 || report.DatabaseOverLimitAfter || firstOfflineWarning)
                    {
                        Logging.Append($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] JOB RETENTION: deleted={report.TotalDeleted} age={report.DeletedByAge} count={report.DeletedByCount} payload={report.DeletedByPayload} remainingTerminal={report.RemainingTerminalJobs} terminalPayloadMiB={report.TerminalPayloadBytesAfter / 1048576.0:F1} databaseMiB={report.DatabaseBytesAfter / 1048576.0:F1} freeMiB={report.FreelistBytesAfter / 1048576.0:F1} autoVacuum={report.AutoVacuumModeAfter} offlineCompactionRequired={report.OfflineCompactionRequired} hasMore={report.HasMore}");
                    }
                    _offlineCompactionWarningLogged = report.OfflineCompactionRequired;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Logging.Append($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] JOB RETENTION WARN: {ex.Message}");
                }

                try
                {
                    await Task.Delay(runSoon ? TimeSpan.FromSeconds(5) : TimeSpan.FromMinutes(_intervalMinutes), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        private static int ReadInt(IConfiguration configuration, string key, string environmentKey, int fallback, int min, int max)
        {
            var text = Environment.GetEnvironmentVariable(environmentKey);
            if (string.IsNullOrWhiteSpace(text)) text = configuration[key];
            return int.TryParse(text, out var value) ? Math.Max(min, Math.Min(max, value)) : fallback;
        }

        private static long ReadLong(IConfiguration configuration, string key, string environmentKey, long fallback, long min, long max)
        {
            var text = Environment.GetEnvironmentVariable(environmentKey);
            if (string.IsNullOrWhiteSpace(text)) text = configuration[key];
            return long.TryParse(text, out var value) ? Math.Max(min, Math.Min(max, value)) : fallback;
        }

        private static bool ReadBool(IConfiguration configuration, string key, string environmentKey, bool fallback)
        {
            var text = Environment.GetEnvironmentVariable(environmentKey);
            if (string.IsNullOrWhiteSpace(text)) text = configuration[key];
            return bool.TryParse(text, out var value) ? value : fallback;
        }
    }
}

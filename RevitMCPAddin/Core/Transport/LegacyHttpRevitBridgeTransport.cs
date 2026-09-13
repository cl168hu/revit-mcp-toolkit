#nullable enable
using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RevitMCPAddin.Core.Transport
{
    /// <summary>
    /// Existing localhost HTTP bridge expressed through the transport-neutral NP-0
    /// contract.  Endpoint behavior is intentionally unchanged:
    /// pending_request long-poll, heartbeat GET, jobs diagnostic, and post_result.
    /// </summary>
    internal sealed class LegacyHttpRevitBridgeTransport : IRevitBridgeTransport
    {
        private readonly HttpClient _pollClient;
        private readonly HttpClient _resultClient;
        private readonly object _addressLock = new object();
        private Uri _baseAddress;

        public LegacyHttpRevitBridgeTransport(HttpClient pollClient, HttpClient resultClient, string baseAddress)
        {
            _pollClient = pollClient ?? throw new ArgumentNullException(nameof(pollClient));
            _resultClient = resultClient ?? throw new ArgumentNullException(nameof(resultClient));
            _baseAddress = NormalizeBaseAddress(baseAddress);
        }

        public string Name => "legacyHttp";

        public string BaseAddress
        {
            get
            {
                lock (_addressLock)
                {
                    return _baseAddress.AbsoluteUri;
                }
            }
        }

        public async Task<BridgeReceiveResult> ReceiveAsync(int waitMs, CancellationToken cancellationToken)
        {
            if (waitMs < 0) waitMs = 0;
            var sw = Stopwatch.StartNew();
            using var response = await _pollClient.GetAsync(
                BuildUri("pending_request?waitMs=" + waitMs),
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken).ConfigureAwait(false);

            string jobId = string.Empty;
            try
            {
                if (response.Headers.TryGetValues("X-Job-Id", out var values))
                {
                    foreach (var value in values)
                    {
                        jobId = value ?? string.Empty;
                        break;
                    }
                }
            }
            catch { /* best-effort compatibility header */ }

            string payload = string.Empty;
            if (response.IsSuccessStatusCode && response.StatusCode != HttpStatusCode.NoContent)
            {
                payload = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            }

            sw.Stop();
            return new BridgeReceiveResult
            {
                TransportName = Name,
                IsSuccessStatusCode = response.IsSuccessStatusCode,
                IsNoContent = response.StatusCode == HttpStatusCode.NoContent,
                StatusCode = (int)response.StatusCode,
                JobId = jobId,
                Payload = payload ?? string.Empty,
                ElapsedMs = sw.ElapsedMilliseconds
            };
        }

        public async Task<bool> HasEnqueuedJobsAsync(CancellationToken cancellationToken)
        {
            using var response = await _pollClient.GetAsync(
                BuildUri("jobs?state=ENQUEUED&limit=1"),
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode) return false;

            var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            // Preserve the pre-NP-0 diagnostic behavior. This is not job dispatch.
            return !string.IsNullOrWhiteSpace(body) && body.TrimStart().StartsWith("[", StringComparison.Ordinal);
        }

        public async Task SendHeartbeatAsync(string rpcId, string jobId, CancellationToken cancellationToken)
        {
            string query = !string.IsNullOrWhiteSpace(jobId)
                ? "jobId=" + Uri.EscapeDataString(jobId)
                : (!string.IsNullOrWhiteSpace(rpcId) ? "rpcId=" + Uri.EscapeDataString(rpcId) : string.Empty);
            if (string.IsNullOrWhiteSpace(query)) return;

            using var response = await _pollClient.GetAsync(
                BuildUri("heartbeat?" + query),
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken).ConfigureAwait(false);
        }

        public async Task<BridgeSendResult> SendResultAsync(string jsonBody, CancellationToken cancellationToken)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                using var content = new StringContent(jsonBody ?? string.Empty, Encoding.UTF8, "application/json");
                using var response = await _resultClient.PostAsync(
                    BuildUri("post_result"),
                    content,
                    cancellationToken).ConfigureAwait(false);
                sw.Stop();
                return new BridgeSendResult
                {
                    TransportName = Name,
                    IsSuccessStatusCode = response.IsSuccessStatusCode,
                    StatusCode = (int)response.StatusCode,
                    ElapsedMs = sw.ElapsedMilliseconds
                };
            }
            catch (Exception ex)
            {
                sw.Stop();
                return new BridgeSendResult
                {
                    TransportName = Name,
                    IsSuccessStatusCode = false,
                    StatusCode = 0,
                    ElapsedMs = sw.ElapsedMilliseconds,
                    Error = ex.Message
                };
            }
        }

        public void SetBaseAddress(string baseAddress)
        {
            if (string.IsNullOrWhiteSpace(baseAddress)) return;
            var normalized = NormalizeBaseAddress(baseAddress);
            lock (_addressLock)
            {
                _baseAddress = normalized;
            }
        }

        private Uri BuildUri(string relative)
        {
            lock (_addressLock)
            {
                return new Uri(_baseAddress, relative ?? string.Empty);
            }
        }

        private static Uri NormalizeBaseAddress(string baseAddress)
        {
            if (!Uri.TryCreate(baseAddress, UriKind.Absolute, out var uri))
                throw new ArgumentException("A valid absolute base address is required.", nameof(baseAddress));

            var text = uri.AbsoluteUri;
            if (!text.EndsWith("/", StringComparison.Ordinal)) text += "/";
            return new Uri(text, UriKind.Absolute);
        }
    }
}

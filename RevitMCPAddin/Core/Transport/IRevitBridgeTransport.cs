#nullable enable
using System.Threading;
using System.Threading.Tasks;

namespace RevitMCPAddin.Core.Transport
{
    /// <summary>
    /// Transport-neutral result of waiting for one command from RevitMCPServer.
    /// Revit execution remains owned by RevitMcpWorker and its existing ExternalEvent queue.
    /// </summary>
    internal sealed class BridgeReceiveResult
    {
        public string TransportName { get; set; } = string.Empty;
        public bool IsSuccessStatusCode { get; set; }
        public bool IsNoContent { get; set; }
        public int StatusCode { get; set; }
        public string JobId { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public long ElapsedMs { get; set; }

        public bool HasWork => IsSuccessStatusCode && !IsNoContent && !string.IsNullOrWhiteSpace(Payload);
    }

    /// <summary>
    /// Transport-neutral result of sending a final JSON-RPC result to RevitMCPServer.
    /// </summary>
    internal sealed class BridgeSendResult
    {
        public string TransportName { get; set; } = string.Empty;
        public bool IsSuccessStatusCode { get; set; }
        public int StatusCode { get; set; }
        public long ElapsedMs { get; set; }
        public string Error { get; set; } = string.Empty;

        public bool PayloadTooLarge => StatusCode == 413;
    }

    /// <summary>
    /// Server/Add-in bridge used by the worker.  NP-0 has only the legacy HTTP
    /// implementation; a named-pipe implementation can be added without changing
    /// command handlers, the FIFO dispatch queue, or ResultDeliveryService.
    /// </summary>
    internal interface IRevitBridgeTransport
    {
        string Name { get; }
        string BaseAddress { get; }

        Task<BridgeReceiveResult> ReceiveAsync(int waitMs, CancellationToken cancellationToken);
        Task<bool> HasEnqueuedJobsAsync(CancellationToken cancellationToken);
        Task SendHeartbeatAsync(string rpcId, string jobId, CancellationToken cancellationToken);
        Task<BridgeSendResult> SendResultAsync(string jsonBody, CancellationToken cancellationToken);

        void SetBaseAddress(string baseAddress);
    }
}

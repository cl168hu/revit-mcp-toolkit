#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using RevitMCPAddin.Core.Transport;

namespace RevitMCPAddin.Core.ResultDelivery
{
    internal sealed class ResultDeliveryService : IResultDeliveryService
    {
        private readonly IRevitBridgeTransport _transport;
        private readonly Action<string>? _onDelivered;
        private readonly ConcurrentQueue<PendingResultItem> _queue = new ConcurrentQueue<PendingResultItem>();
        private readonly SemaphoreSlim _signal = new SemaphoreSlim(0);
        private readonly object _lifecycleLock = new object();
        private readonly object _activeItemLock = new object();
        private CancellationTokenSource? _cts;
        private Task? _loop;
        private FileBackedPendingResultStore _store;
        private PendingResultItem? _activeItem;
        private int _storePort;
        private bool _started;

        private const int PersistAfterAttempt = 5;
        // Keep below the server's 32 MB request limit. UTF-8 byte count is used, not char count.
        private const int MaxInlineResultBytes = 30_000_000;

        public ResultDeliveryService(IRevitBridgeTransport transport, int port, Action<string>? onDelivered)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _store = new FileBackedPendingResultStore(port);
            _storePort = port;
            _onDelivered = onDelivered;
        }

        public void SetBaseAddress(string baseAddress)
        {
            if (string.IsNullOrWhiteSpace(baseAddress)) return;
            try
            {
                _transport.SetBaseAddress(baseAddress);
            }
            catch (Exception ex)
            {
                RevitLogger.Warn($"[RESULT] SetBaseAddress failed: {ex.Message}");
            }
        }

        public void UpdatePort(int port)
        {
            if (port <= 0) return;
            lock (_lifecycleLock)
            {
                if (_storePort == port) return;
                _store = new FileBackedPendingResultStore(port);
                _storePort = port;
            }
            RevitLogger.Info($"[RESULT] switched pending result store to port={port}");
        }

        public void Start()
        {
            lock (_lifecycleLock)
            {
                if (_started) return;
                _started = true;
                _cts = new CancellationTokenSource();
                _loop = Task.Run(() => LoopAsync(_cts.Token));
            }

            try
            {
                var persisted = _store.LoadAll();
                foreach (var it in persisted)
                {
                    // A restart is a fresh delivery opportunity. Retaining a large historic
                    // backoff here would block new results for many seconds at startup.
                    it.Attempt = 0;
                    _queue.Enqueue(it);
                    _signal.Release();
                }
                if (persisted.Count > 0)
                {
                    RevitLogger.Info($"[RESULT] loaded pending results: {persisted.Count}");
                }
            }
            catch (Exception ex)
            {
                RevitLogger.Warn($"[RESULT] loading pending results failed: {ex.Message}");
            }
        }

        public void Stop()
        {
            Task? loop;
            lock (_lifecycleLock)
            {
                if (!_started) return;
                _started = false;
                try { _cts?.Cancel(); } catch { /* ignore */ }
                loop = _loop;
                _loop = null;
            }

            try { loop?.Wait(5000); } catch { /* ignore */ }
            PersistOutstandingForShutdown();
            try { _cts?.Dispose(); } catch { /* ignore */ }
            _cts = null;
        }

        public void Enqueue(PendingResultItem item)
        {
            if (item == null || string.IsNullOrWhiteSpace(item.JsonBody)) return;
            _queue.Enqueue(item);
            _signal.Release();
        }

        private async Task LoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await _signal.WaitAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (!_queue.TryDequeue(out var item) || item == null)
                {
                    continue;
                }

                SetActiveItem(item);

                ReduceOversizedPayloadIfNeeded(item, "local hard limit");

                var delayMs = GetRetryDelayMs(item.Attempt);
                if (delayMs > 0)
                {
                    try { await Task.Delay(delayMs, ct).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        PersistItemForShutdown(item, forceCurrentStore: true);
                        ClearActiveItem(item);
                        break;
                    }
                }

                try
                {
                    var delivery = await _transport.SendResultAsync(item.JsonBody, ct).ConfigureAwait(false);
                    if (!delivery.IsSuccessStatusCode)
                    {
                        if (delivery.PayloadTooLarge && !item.PayloadReduced)
                        {
                            ReduceOversizedPayloadIfNeeded(item, "server returned payload-too-large", force: true);
                            ClearActiveItem(item);
                            _queue.Enqueue(item);
                            _signal.Release();
                            continue;
                        }
                        var detail = !string.IsNullOrWhiteSpace(delivery.Error)
                            ? delivery.Error
                            : ("status=" + delivery.StatusCode);
                        throw new HttpRequestException(_transport.Name + " result delivery failed: " + detail);
                    }

                    // Success: clear persisted copy and stop heartbeat.
                    if (!string.IsNullOrWhiteSpace(item.StorePath))
                    {
                        _store.Delete(item.StorePath);
                        item.StorePath = null;
                    }

                    var hb = !string.IsNullOrWhiteSpace(item.HeartbeatKey) ? item.HeartbeatKey : item.RpcId;
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(hb)) _onDelivered?.Invoke(hb);
                    }
                    catch { /* ignore heartbeat callback errors */ }

                    RevitLogger.Info($"[RESULT] delivered transport={delivery.TransportName} rpcId={item.RpcId} attempt={item.Attempt + 1} transportMs={delivery.ElapsedMs}");
                    ClearActiveItem(item);
                }
                catch (OperationCanceledException)
                {
                    PersistItemForShutdown(item, forceCurrentStore: true);
                    ClearActiveItem(item);
                    break;
                }
                catch (Exception ex)
                {
                    item.Attempt++;
                    item.LastError = ex.Message;
                    if (item.Attempt >= PersistAfterAttempt && string.IsNullOrWhiteSpace(item.StorePath))
                    {
                        try
                        {
                            item.StorePath = _store.Save(item);
                            RevitLogger.Warn($"[RESULT] persisted rpcId={item.RpcId} attempt={item.Attempt}");
                        }
                        catch (Exception saveEx)
                        {
                            RevitLogger.Warn($"[RESULT] persist failed rpcId={item.RpcId}: {saveEx.Message}");
                        }
                    }

                    RevitLogger.Warn($"[RESULT] delivery failed rpcId={item.RpcId} attempt={item.Attempt}: {item.LastError}");
                    ClearActiveItem(item);
                    _queue.Enqueue(item);
                    _signal.Release();
                }
            }
        }

        private void SetActiveItem(PendingResultItem item)
        {
            lock (_activeItemLock)
            {
                _activeItem = item;
            }
        }

        private void ClearActiveItem(PendingResultItem item)
        {
            lock (_activeItemLock)
            {
                if (ReferenceEquals(_activeItem, item))
                {
                    _activeItem = null;
                }
            }
        }

        private void PersistOutstandingForShutdown()
        {
            var pending = new List<PendingResultItem>();
            lock (_activeItemLock)
            {
                if (_activeItem != null)
                {
                    pending.Add(_activeItem);
                    _activeItem = null;
                }
            }

            while (_queue.TryDequeue(out var queued))
            {
                if (queued != null)
                {
                    pending.Add(queued);
                }
            }

            int persisted = 0;
            foreach (var item in pending)
            {
                if (PersistItemForShutdown(item, forceCurrentStore: true))
                {
                    persisted++;
                }
            }

            if (persisted > 0)
            {
                RevitLogger.Warn($"[RESULT] shutdown persisted pending results: {persisted}");
            }
        }

        private bool PersistItemForShutdown(PendingResultItem item, bool forceCurrentStore)
        {
            if (item == null || string.IsNullOrWhiteSpace(item.JsonBody)) return false;

            try
            {
                var store = _store;
                var oldPath = item.StorePath;
                var needsSave = string.IsNullOrWhiteSpace(oldPath) || (forceCurrentStore && !store.OwnsPath(oldPath));
                if (!needsSave)
                {
                    return false;
                }

                item.StorePath = store.Save(item);
                if (!string.IsNullOrWhiteSpace(oldPath) &&
                    !string.Equals(oldPath, item.StorePath, StringComparison.OrdinalIgnoreCase))
                {
                    store.Delete(oldPath);
                }
                return true;
            }
            catch (Exception ex)
            {
                RevitLogger.Warn($"[RESULT] shutdown persist failed rpcId={item.RpcId}: {ex.Message}");
                return false;
            }
        }

        private static int GetRetryDelayMs(int attempt)
        {
            if (attempt <= 0) return 0;
            if (attempt == 1) return 250;
            if (attempt == 2) return 1000;
            if (attempt == 3) return 3000;
            return 10000;
        }

        private void ReduceOversizedPayloadIfNeeded(PendingResultItem item, string reason, bool force = false)
        {
            if (item == null || item.PayloadReduced || string.IsNullOrWhiteSpace(item.JsonBody)) return;

            int bytes;
            try { bytes = Encoding.UTF8.GetByteCount(item.JsonBody); }
            catch { bytes = item.JsonBody.Length * 2; }
            if (!force && bytes <= MaxInlineResultBytes) return;

            // Preserve the original envelope in the retry store until the compact response
            // reaches the server. The persisted copy is deleted after successful delivery.
            if (string.IsNullOrWhiteSpace(item.StorePath))
            {
                try { item.StorePath = _store.Save(item); }
                catch (Exception ex) { RevitLogger.Warn($"[RESULT] oversized payload persistence failed rpcId={item.RpcId}: {ex.Message}"); }
            }

            item.OriginalPayloadBytes = bytes;
            item.PayloadReduced = true;
            item.Attempt = 0;
            item.LastError = reason;

            var envelope = new JObject
            {
                ["jsonrpc"] = "2.0",
                ["id"] = item.RpcId ?? string.Empty,
                ["result"] = new JObject
                {
                    ["ok"] = false,
                    ["code"] = "RESULT_TOO_LARGE",
                    ["msg"] = "The Revit command completed, but its result was too large for safe inline delivery. Rerun with paging, summaryOnly, or a file-export option.",
                    ["originalPayloadBytes"] = bytes,
                    ["deliveryLimitBytes"] = MaxInlineResultBytes
                }
            };
            item.JsonBody = JsonNetCompat.ToCompactJson(envelope);
            RevitLogger.Warn($"[RESULT] oversized payload reduced rpcId={item.RpcId} bytes={bytes} reason={reason}");
        }
    }
}

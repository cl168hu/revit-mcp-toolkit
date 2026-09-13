#nullable enable
using Autodesk.Revit.DB;
using Autodesk.Revit.UI;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using RevitMCPAddin.Core.Net;

namespace RevitMCPAddin.Core
{
    internal static class RevitInstanceStateService
    {
        private static readonly object Sync = new object();
        private static string Root => Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "RevitMCP");
        private static string LastRevitVersion = string.Empty;
        private static string LastDocTitle = string.Empty;
        private static string LastDocGuid = string.Empty;
        private static string LastDocPath = string.Empty;
        private static string LastStartedUtc = string.Empty;
        private static string BridgePipeName = string.Empty;
        private static string BridgeProtocol = string.Empty;
        private static string BridgeStatus = "legacyHttp";
        private static int BridgeServerProcessId;
        private static readonly TimeSpan PublishHeartbeat = TimeSpan.FromSeconds(10);
        private static DateTime LastPublishPreparedUtc = DateTime.MinValue;
        private static RevitInstanceState? PendingInstance;
        private static bool WriterRunning;
        private static bool ShuttingDown;

        public static void SetBridgeState(
            int port,
            string pipeName,
            string protocol,
            string status,
            int serverProcessId)
        {
            lock (Sync)
            {
                BridgePipeName = pipeName ?? string.Empty;
                BridgeProtocol = protocol ?? string.Empty;
                BridgeStatus = string.IsNullOrWhiteSpace(status) ? "legacyHttp" : status;
                BridgeServerProcessId = serverProcessId;
            }
            if (port > 0 && port <= 65535)
                PublishCore(null, null, port, force: true);
        }

        public static void Publish(UIControlledApplication? app, int port, bool force = false)
        {
            PublishCore(null, app?.ControlledApplication?.VersionNumber, port, force);
        }

        public static void Publish(Document? doc, int port, bool force = false)
        {
            PublishCore(doc, null, port, force);
        }

        public static void RemoveCurrentProcess()
        {
            try
            {
                lock (Sync)
                {
                    ShuttingDown = true;
                    PendingInstance = null;
                }
                using (var registryGate = RevitInstanceRegistryGate.TryEnter(5000))
                {
                    if (registryGate == null)
                    {
                        RevitLogger.Warn("Revit instance registry is busy; current-process removal was skipped safely.");
                        return;
                    }
                    lock (Sync)
                    {
                        var pid = Process.GetCurrentProcess().Id;
                        Directory.CreateDirectory(Root);
                        var procPath = Path.Combine(Root, $"server_state_{pid}.json");
                        if (File.Exists(procPath)) File.Delete(procPath);
                        var aggregatePath = Path.Combine(Root, "server_state.json");
                        var list = ReadInstances(aggregatePath);
                        list.RemoveAll(x => x.ProcessId == pid);
                        WriteAggregate(aggregatePath, list);
                    }
                }
            }
            catch (Exception ex)
            {
                RevitLogger.Warn("RevitInstanceStateService.RemoveCurrentProcess failed: " + ex.Message);
            }
        }

        private static void PublishCore(Document? doc, string? revitVersion, int port, bool force)
        {
            if (port <= 0 || port > 65535) return;
            try
            {
                var nowUtc = DateTime.UtcNow;
                lock (Sync)
                {
                    if (ShuttingDown) return;
                    if (!force &&
                        LastPublishPreparedUtc != DateTime.MinValue &&
                        nowUtc - LastPublishPreparedUtc < PublishHeartbeat)
                        return;
                    LastPublishPreparedUtc = nowUtc;
                }

                // Build the immutable snapshot while still on the Revit API thread. Only
                // filesystem/mutex work is delegated to the background writer below.
                var instance = BuildInstance(doc, revitVersion, port);
                QueuePublish(instance);
            }
            catch (Exception ex)
            {
                RevitLogger.Warn("RevitInstanceStateService.Publish failed: " + ex.Message);
            }
        }

        private static void QueuePublish(RevitInstanceState instance)
        {
            bool startWriter = false;
            lock (Sync)
            {
                if (ShuttingDown) return;
                // Latest state wins. This prevents a burst of view/document events from
                // creating an equally large burst of disk writes.
                PendingInstance = instance;
                if (!WriterRunning)
                {
                    WriterRunning = true;
                    startWriter = true;
                }
            }

            if (!startWriter) return;
            try
            {
                Task.Run((Action)ProcessPublishQueue);
            }
            catch (Exception ex)
            {
                lock (Sync) WriterRunning = false;
                RevitLogger.Warn("RevitInstanceStateService writer start failed: " + ex.Message);
            }
        }

        private static void ProcessPublishQueue()
        {
            while (true)
            {
                RevitInstanceState? instance;
                lock (Sync)
                {
                    if (ShuttingDown)
                    {
                        PendingInstance = null;
                        WriterRunning = false;
                        return;
                    }
                    instance = PendingInstance;
                    PendingInstance = null;
                    if (instance == null)
                    {
                        WriterRunning = false;
                        return;
                    }
                }

                WriteInstance(instance);
            }
        }

        private static void WriteInstance(RevitInstanceState instance)
        {
            try
            {
                using (var registryGate = RevitInstanceRegistryGate.TryEnter(5000))
                {
                    if (registryGate == null)
                    {
                        RevitLogger.Warn("Revit instance registry is busy; publish was skipped safely and will retry on the next state update.");
                        return;
                    }
                    lock (Sync)
                    {
                        if (ShuttingDown) return;
                    }

                    Directory.CreateDirectory(Root);
                    File.WriteAllText(
                        Path.Combine(Root, $"server_state_{instance.ProcessId}.json"),
                        instance.ToJson(),
                        Encoding.UTF8);

                    var aggregatePath = Path.Combine(Root, "server_state.json");
                    var list = ReadInstances(aggregatePath);
                    var now = DateTimeOffset.UtcNow;
                    list.RemoveAll(x => x.ProcessId == instance.ProcessId || x.Port == instance.Port || IsStale(x, now));
                    list.Add(instance);
                    WriteAggregate(aggregatePath, list);
                }
            }
            catch (Exception ex)
            {
                RevitLogger.Warn("RevitInstanceStateService background publish failed: " + ex.Message);
            }
        }

        private static RevitInstanceState BuildInstance(Document? doc, string? revitVersion, int port)
        {
            var pid = Process.GetCurrentProcess().Id;
            var now = DateTimeOffset.UtcNow.ToString("o", CultureInfo.InvariantCulture);
            var suppliedVersion = string.IsNullOrWhiteSpace(revitVersion)
                ? Safe(() => doc?.Application?.VersionNumber) ?? string.Empty
                : revitVersion ?? string.Empty;

            var title = Safe(() => doc?.Title) ?? string.Empty;
            var path = Safe(() => doc?.PathName) ?? string.Empty;
            var docGuid = string.Empty;
            if (doc != null)
            {
                try
                {
                    string source;
                    docGuid = DocumentKeyUtil.GetDocKeyOrStable(doc, createIfMissing: true, out source);
                }
                catch { }
                if (string.IsNullOrWhiteSpace(docGuid))
                {
                    try { docGuid = doc.ProjectInformation?.UniqueId ?? string.Empty; } catch { }
                }
            }

            lock (Sync)
            {
                if (!string.IsNullOrWhiteSpace(suppliedVersion))
                    LastRevitVersion = suppliedVersion;
                if (doc != null)
                {
                    LastDocTitle = title;
                    LastDocPath = path;
                    LastDocGuid = docGuid ?? string.Empty;
                }
                else
                {
                    title = LastDocTitle;
                    path = LastDocPath;
                    docGuid = LastDocGuid;
                }
                if (string.IsNullOrWhiteSpace(LastStartedUtc))
                    LastStartedUtc = now;

                return new RevitInstanceState
                {
                    Port = port,
                    Endpoint = $"http://127.0.0.1:{port}/rpc",
                    Health = $"http://127.0.0.1:{port}/health",
                    ProcessId = pid,
                    RevitVersion = LastRevitVersion,
                    DocTitle = title,
                    DocGuid = docGuid ?? string.Empty,
                    DocPath = path,
                    StartedUtc = LastStartedUtc,
                    LastSeenUtc = now,
                    BridgePipeName = BridgePipeName,
                    BridgeProtocol = BridgeProtocol,
                    BridgeStatus = BridgeStatus,
                    BridgeServerProcessId = BridgeServerProcessId
                };
            }
        }

        private static bool IsStale(RevitInstanceState state, DateTimeOffset now)
        {
            if (state.ProcessId > 0)
            {
                try { Process.GetProcessById(state.ProcessId); }
                catch { return true; }
            }
            if (DateTimeOffset.TryParse(state.LastSeenUtc, out var last))
                return (now - last).TotalMinutes > 10;
            return false;
        }

        private static List<RevitInstanceState> ReadInstances(string path)
        {
            var result = new List<RevitInstanceState>();
            try
            {
                if (!File.Exists(path)) return result;
                var text = File.ReadAllText(path);
                foreach (Match m in Regex.Matches(text, "\\{[^{}]*\\}"))
                {
                    var obj = m.Value;
                    var port = ReadInt(obj, "port");
                    var pid = ReadInt(obj, "processId");
                    if (pid == 0) pid = ReadInt(obj, "pid");
                    if (port <= 0 || pid <= 0) continue;
                    result.Add(new RevitInstanceState
                    {
                        Port = port,
                        Endpoint = ReadString(obj, "endpoint"),
                        Health = ReadString(obj, "health"),
                        ProcessId = pid,
                        RevitVersion = ReadString(obj, "revitVersion"),
                        DocTitle = ReadString(obj, "docTitle"),
                        DocGuid = ReadString(obj, "docGuid"),
                        DocPath = ReadString(obj, "docPath"),
                        StartedUtc = ReadString(obj, "startedUtc"),
                        LastSeenUtc = ReadString(obj, "lastSeenUtc"),
                        BridgePipeName = ReadString(obj, "bridgePipeName"),
                        BridgeProtocol = ReadString(obj, "bridgeProtocol"),
                        BridgeStatus = ReadString(obj, "bridgeStatus"),
                        BridgeServerProcessId = ReadInt(obj, "bridgeServerProcessId")
                    });
                }
            }
            catch { }
            return result;
        }

        private static int ReadInt(string json, string name)
        {
            var m = Regex.Match(json, "\"" + Regex.Escape(name) + "\"\\s*:\\s*(\\d+)", RegexOptions.IgnoreCase);
            return m.Success && int.TryParse(m.Groups[1].Value, out var v) ? v : 0;
        }

        private static string ReadString(string json, string name)
        {
            var m = Regex.Match(json, "\"" + Regex.Escape(name) + "\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"", RegexOptions.IgnoreCase);
            return m.Success ? Unescape(m.Groups[1].Value) : string.Empty;
        }

        private static void WriteAggregate(string path, List<RevitInstanceState> instances)
        {
            RevitInstanceState? current = null;
            if (instances.Count > 0)
            {
                current = instances[instances.Count - 1];
            }

            var sb = new StringBuilder();
            sb.Append("{\"schemaVersion\":3");
            if (current != null)
            {
                sb.Append(",\"port\":").Append(current.Port.ToString(CultureInfo.InvariantCulture))
                  .Append(",\"pid\":").Append(current.ProcessId.ToString(CultureInfo.InvariantCulture))
                  .Append(",\"processId\":").Append(current.ProcessId.ToString(CultureInfo.InvariantCulture))
                  .Append(",\"endpoint\":\"").Append(Escape(current.Endpoint)).Append("\"")
                  .Append(",\"health\":\"").Append(Escape(current.Health)).Append("\"")
                  .Append(",\"bridgeStatus\":\"").Append(Escape(current.BridgeStatus)).Append("\"")
                  .Append(",\"bridgeProtocol\":\"").Append(Escape(current.BridgeProtocol)).Append("\"");
            }

            sb.Append(",\"updatedUtc\":\"")
              .Append(Escape(DateTimeOffset.UtcNow.ToString("o", CultureInfo.InvariantCulture)))
              .Append("\",\"instances\":[");
            for (int i = 0; i < instances.Count; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append(instances[i].ToJson());
            }
            sb.Append("]}");
            File.WriteAllText(path, sb.ToString(), Encoding.UTF8);
        }

        private static string? Safe(Func<string?> getter)
        {
            try { return getter(); } catch { return null; }
        }

        private static string Escape(string? value)
        {
            if (string.IsNullOrEmpty(value)) return string.Empty;
            return value.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\r", "\\r").Replace("\n", "\\n");
        }

        private static string Unescape(string value)
        {
            return (value ?? string.Empty).Replace("\\n", "\n").Replace("\\r", "\r").Replace("\\\"", "\"").Replace("\\\\", "\\");
        }

        private sealed class RevitInstanceState
        {
            public int Port { get; set; }
            public string Endpoint { get; set; } = string.Empty;
            public string Health { get; set; } = string.Empty;
            public int ProcessId { get; set; }
            public string RevitVersion { get; set; } = string.Empty;
            public string DocTitle { get; set; } = string.Empty;
            public string DocGuid { get; set; } = string.Empty;
            public string DocPath { get; set; } = string.Empty;
            public string StartedUtc { get; set; } = string.Empty;
            public string LastSeenUtc { get; set; } = string.Empty;
            public string BridgePipeName { get; set; } = string.Empty;
            public string BridgeProtocol { get; set; } = string.Empty;
            public string BridgeStatus { get; set; } = string.Empty;
            public int BridgeServerProcessId { get; set; }

            public string ToJson()
            {
                return "{\"port\":" + Port.ToString(CultureInfo.InvariantCulture)
                    + ",\"pid\":" + ProcessId.ToString(CultureInfo.InvariantCulture)
                    + ",\"processId\":" + ProcessId.ToString(CultureInfo.InvariantCulture)
                    + ",\"endpoint\":\"" + Escape(Endpoint) + "\""
                    + ",\"health\":\"" + Escape(Health) + "\""
                    + ",\"revitVersion\":\"" + Escape(RevitVersion) + "\""
                    + ",\"docTitle\":\"" + Escape(DocTitle) + "\""
                    + ",\"docGuid\":\"" + Escape(DocGuid) + "\""
                    + ",\"docPath\":\"" + Escape(DocPath) + "\""
                    + ",\"startedAtUtc\":\"" + Escape(StartedUtc) + "\""
                    + ",\"startedUtc\":\"" + Escape(StartedUtc) + "\""
                    + ",\"lastSeenUtc\":\"" + Escape(LastSeenUtc) + "\""
                    + ",\"bridgePipeName\":\"" + Escape(BridgePipeName) + "\""
                    + ",\"bridgeProtocol\":\"" + Escape(BridgeProtocol) + "\""
                    + ",\"bridgeStatus\":\"" + Escape(BridgeStatus) + "\""
                    + ",\"bridgeServerProcessId\":" + BridgeServerProcessId.ToString(CultureInfo.InvariantCulture) + "}";
            }
        }
    }
}

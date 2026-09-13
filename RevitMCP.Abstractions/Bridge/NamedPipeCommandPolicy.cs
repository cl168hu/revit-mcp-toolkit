#nullable enable
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text.Json;

namespace RevitMCP.Abstractions.Bridge
{
    /// <summary>
    /// Shared command classification for the staged named-pipe rollout.
    /// Public builds route only the explicit read-only allowlist through the
    /// named-pipe fast path. Write experiments remain out of the published
    /// command set until they are separately reviewed.
    /// </summary>
    public static class NamedPipeCommandPolicy
    {
        public const string ReadOnlyClass = "readOnly";
        public const string DryRunWriteClass = "dryRunWrite";

        private static readonly ReadOnlyCollection<string> DryRunWriteMethodList
            = Array.AsReadOnly(Array.Empty<string>());

        private static readonly HashSet<string> DryRunWriteMethodSet
            = new HashSet<string>(DryRunWriteMethodList, StringComparer.OrdinalIgnoreCase);

        public static IReadOnlyCollection<string> DryRunWriteMethods => DryRunWriteMethodList;

        public static bool IsDryRunWriteMethod(string? method)
        {
            return !string.IsNullOrWhiteSpace(method)
                && DryRunWriteMethodSet.Contains((method ?? string.Empty).Trim());
        }

        public static bool IsExplicitDryRunWrite(string? method, string? paramsJson)
        {
            if (!IsDryRunWriteMethod(method) || string.IsNullOrWhiteSpace(paramsJson))
                return false;

            try
            {
                var json = paramsJson ?? string.Empty;
                using (var document = JsonDocument.Parse(json))
                {
                    var root = document.RootElement;
                    return root.ValueKind == JsonValueKind.Object
                        && root.TryGetProperty("dryRun", out var dryRun)
                        && dryRun.ValueKind == JsonValueKind.True;
                }
            }
            catch
            {
                return false;
            }
        }

        public static string Classify(
            string? method,
            string? paramsJson,
            bool readOnlyEnabled,
            bool dryRunWriteEnabled)
        {
            if (readOnlyEnabled && NamedPipeReadOnlyAllowlist.Contains(method))
                return ReadOnlyClass;

            if (dryRunWriteEnabled && IsExplicitDryRunWrite(method, paramsJson))
                return DryRunWriteClass;

            return string.Empty;
        }
    }
}

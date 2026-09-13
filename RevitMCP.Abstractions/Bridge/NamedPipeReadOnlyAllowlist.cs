#nullable enable
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace RevitMCP.Abstractions.Bridge
{
    /// <summary>
    /// Canonical read-only methods that are safe to reserve for the local
    /// named-pipe bridge. Legacy aliases deliberately remain on HTTP so the
    /// old transport stays available for fallback and A/B comparison.
    ///
    /// Keep this list in the shared abstractions assembly so the server and
    /// every Revit add-in target enforce exactly the same boundary.
    /// </summary>
    public static class NamedPipeReadOnlyAllowlist
    {
        private static readonly ReadOnlyCollection<string> CanonicalMethodList
            = Array.AsReadOnly(new[]
            {
                "element.get_rooms",
                "element.get_levels",
                "element.get_room_params",
                "element.get_element_info"
            });

        private static readonly HashSet<string> CanonicalMethodSet
            = new HashSet<string>(CanonicalMethodList, StringComparer.OrdinalIgnoreCase);

        private static readonly ReadOnlyDictionary<string, string> LegacyAliasMap
            = new ReadOnlyDictionary<string, string>(
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["element.get_rooms"] = "get_rooms",
                    ["element.get_levels"] = "get_levels",
                    ["element.get_room_params"] = "get_room_params",
                    ["element.get_element_info"] = "get_element_info"
                });

        public static IReadOnlyCollection<string> CanonicalMethods => CanonicalMethodList;

        public static IReadOnlyDictionary<string, string> LegacyAliasesByCanonical => LegacyAliasMap;

        public static bool Contains(string? method)
        {
            return !string.IsNullOrWhiteSpace(method)
                && CanonicalMethodSet.Contains((method ?? string.Empty).Trim());
        }

        public static bool TryGetLegacyAlias(string? canonicalMethod, out string legacyAlias)
        {
            legacyAlias = string.Empty;
            if (string.IsNullOrWhiteSpace(canonicalMethod)) return false;
            if (!LegacyAliasMap.TryGetValue((canonicalMethod ?? string.Empty).Trim(), out var value)) return false;
            legacyAlias = value;
            return true;
        }
    }
}

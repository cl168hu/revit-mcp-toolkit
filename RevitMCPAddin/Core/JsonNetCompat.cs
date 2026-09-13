#nullable enable
using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RevitMCPAddin.Core
{
    /// <summary>
    /// Json.NET compatibility helpers for Revit host environments.
    /// Some hosts load an older Newtonsoft.Json which can miss newer JToken APIs.
    /// Avoid directly calling those potentially-missing methods to prevent MissingMethodException at JIT time.
    /// </summary>
    internal static class JsonNetCompat
    {
        public static string ToCompactJson(object? value)
        {
            if (value == null) return "null";
            try
            {
                // IMPORTANT:
                // Some Revit host environments can end up with mixed Json.NET types/versions.
                // Serializing JToken directly may hit MissingMethodException (WriteTo / ToString overloads).
                if (value is JToken jt)
                    return SerializeToken(jt, indented: false);

                return JsonConvert.SerializeObject(value);
            }
            catch
            {
                return "null";
            }
        }

        public static string ToIndentedJson(object? value)
        {
            if (value == null) return "null";
            try
            {
                if (value is JToken jt)
                    return SerializeToken(jt, indented: true);

                return JsonConvert.SerializeObject(value, Formatting.Indented);
            }
            catch
            {
                return "null";
            }
        }

        /// <summary>
        /// Removes a root JToken from an otherwise plain object graph before the graph is
        /// handed to JsonConvert. This is used for legacy envelopes and operation logs that
        /// embed request/result tokens inside ordinary CLR DTOs.
        /// </summary>
        public static object? ToPlainValue(object? value)
        {
            return value is JToken token ? ToPlainObject(token) : value;
        }

        private static string SerializeToken(JToken token, bool indented)
        {
            var builder = new StringBuilder();
            AppendToken(builder, token, indented, depth: 0);
            return builder.ToString();
        }

        private static void AppendToken(StringBuilder builder, JToken token, bool indented, int depth)
        {
            if (token == null)
            {
                builder.Append("null");
                return;
            }

            if (token is JValue value)
            {
                builder.Append(JsonConvert.SerializeObject(value.Value, Formatting.None));
                return;
            }

            if (token is JObject obj)
            {
                builder.Append('{');
                bool first = true;
                foreach (var property in obj.Properties())
                {
                    if (!first) builder.Append(',');
                    if (indented)
                    {
                        builder.AppendLine();
                        AppendIndent(builder, depth + 1);
                    }

                    builder.Append(JsonConvert.SerializeObject(property.Name ?? string.Empty, Formatting.None));
                    builder.Append(indented ? ": " : ":");
                    AppendToken(builder, property.Value, indented, depth + 1);
                    first = false;
                }

                if (!first && indented)
                {
                    builder.AppendLine();
                    AppendIndent(builder, depth);
                }
                builder.Append('}');
                return;
            }

            if (token is JArray array)
            {
                builder.Append('[');
                bool first = true;
                foreach (var item in array)
                {
                    if (!first) builder.Append(',');
                    if (indented)
                    {
                        builder.AppendLine();
                        AppendIndent(builder, depth + 1);
                    }

                    AppendToken(builder, item, indented, depth + 1);
                    first = false;
                }

                if (!first && indented)
                {
                    builder.AppendLine();
                    AppendIndent(builder, depth);
                }
                builder.Append(']');
                return;
            }

            throw new JsonSerializationException("Unsupported JToken type: " + token.Type);
        }

        private static void AppendIndent(StringBuilder builder, int depth)
        {
            if (depth <= 0) return;
            builder.Append(' ', checked(depth * 2));
        }

        private static object? ToPlainObject(JToken token)
        {
            if (token == null) return null;

            // JValue -> raw CLR value (string/double/bool/DateTime/null)
            if (token is JValue jv) return jv.Value;

            // JObject -> Dictionary<string, object?>
            var jo = token as JObject;
            if (jo != null)
            {
                var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var prop in jo.Properties())
                {
                    var name = prop != null ? (prop.Name ?? string.Empty) : string.Empty;
                    if (string.IsNullOrEmpty(name)) continue;
                    dict[name] = prop != null ? ToPlainObject(prop.Value) : null;
                }
                return dict;
            }

            // JArray -> List<object?>
            var ja = token as JArray;
            if (ja != null)
            {
                var list = new List<object?>(ja.Count);
                foreach (var it in ja)
                    list.Add(ToPlainObject(it));
                return list;
            }

            // Fallback: treat as null (keeps things safe and avoids JToken.ToString/WriteTo paths)
            return null;
        }
    }
}

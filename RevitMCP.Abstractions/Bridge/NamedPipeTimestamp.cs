#nullable enable
using Newtonsoft.Json.Linq;
using System;
using System.Globalization;

namespace RevitMCP.Abstractions.Bridge
{
    /// <summary>
    /// Reads protocol timestamps without losing the fractional seconds that
    /// Json.NET stores inside a Date token. Value&lt;string&gt;() must not be used
    /// for that token because its default formatting can truncate the fraction.
    /// </summary>
    public static class NamedPipeTimestamp
    {
        public static DateTime? ReadUtc(JToken? token)
        {
            if (token == null || token.Type == JTokenType.Null || token.Type == JTokenType.Undefined)
                return null;

            if (token.Type == JTokenType.Date)
            {
                try
                {
                    var value = token.Value<DateTime>();
                    return value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime();
                }
                catch { return null; }
            }

            // Do not call JToken.ToString(Formatting). Revit has one shared
            // AppDomain and another add-in can bind an older Json.NET assembly
            // which does not expose that overload. The missing method is then
            // resolved by the JIT even when a different branch would run.
            string? text = null;
            if (token is JValue scalar)
            {
                if (scalar.Value is string stringValue)
                    text = stringValue;
                else if (scalar.Value is IFormattable formattable)
                    text = formattable.ToString(null, CultureInfo.InvariantCulture);
                else
                    text = Convert.ToString(scalar.Value, CultureInfo.InvariantCulture);
            }
            if (DateTimeOffset.TryParse(
                text,
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out var parsed))
            {
                return parsed.UtcDateTime;
            }
            return null;
        }
    }
}

#nullable enable
using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace RevitMCP.Abstractions.Bridge
{
    /// <summary>
    /// Stable hashes used to bind a persisted Add-in receipt to the exact
    /// Server job. JSON object properties are sorted before hashing so the
    /// Newtonsoft.Json and System.Text.Json hosts produce the same value.
    /// </summary>
    public static class NamedPipeReceiptHash
    {
        public static string ComputeCanonicalJsonSha256(string? json)
        {
            var source = string.IsNullOrWhiteSpace(json) ? "{}" : json!;
            using (var document = JsonDocument.Parse(source))
            using (var stream = new MemoryStream())
            {
                using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions
                {
                    Indented = false,
                    SkipValidation = false
                }))
                {
                    WriteCanonical(writer, document.RootElement);
                    writer.Flush();
                }

                return ComputeBytesSha256(stream.ToArray());
            }
        }

        public static string ComputeUtf8Sha256(string? value)
            => ComputeBytesSha256(Encoding.UTF8.GetBytes(value ?? string.Empty));

        public static bool FixedTimeEquals(string? left, string? right)
        {
            if (string.IsNullOrWhiteSpace(left) || string.IsNullOrWhiteSpace(right))
                return false;

            var leftText = left ?? string.Empty;
            var rightText = right ?? string.Empty;
            var a = Encoding.ASCII.GetBytes(leftText.Trim().ToUpperInvariant());
            var b = Encoding.ASCII.GetBytes(rightText.Trim().ToUpperInvariant());
            if (a.Length != b.Length) return false;

            var difference = 0;
            for (var i = 0; i < a.Length; i++) difference |= a[i] ^ b[i];
            return difference == 0;
        }

        private static string ComputeBytesSha256(byte[] bytes)
        {
            using (var sha = SHA256.Create())
            {
                var hash = sha.ComputeHash(bytes ?? Array.Empty<byte>());
                var builder = new StringBuilder(hash.Length * 2);
                foreach (var value in hash) builder.Append(value.ToString("x2"));
                return builder.ToString();
            }
        }

        private static void WriteCanonical(Utf8JsonWriter writer, JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    writer.WriteStartObject();
                    foreach (var property in element.EnumerateObject().OrderBy(x => x.Name, StringComparer.Ordinal))
                    {
                        writer.WritePropertyName(property.Name);
                        WriteCanonical(writer, property.Value);
                    }
                    writer.WriteEndObject();
                    break;

                case JsonValueKind.Array:
                    writer.WriteStartArray();
                    foreach (var item in element.EnumerateArray()) WriteCanonical(writer, item);
                    writer.WriteEndArray();
                    break;

                case JsonValueKind.String:
                    writer.WriteStringValue(element.GetString());
                    break;

                case JsonValueKind.Number:
                    if (element.TryGetInt64(out var integer)) writer.WriteNumberValue(integer);
                    else if (element.TryGetDecimal(out var decimalValue)) writer.WriteNumberValue(decimalValue);
                    else writer.WriteNumberValue(element.GetDouble());
                    break;

                case JsonValueKind.True:
                    writer.WriteBooleanValue(true);
                    break;

                case JsonValueKind.False:
                    writer.WriteBooleanValue(false);
                    break;

                case JsonValueKind.Null:
                    writer.WriteNullValue();
                    break;

                default:
                    throw new InvalidDataException("Unsupported JSON token in receipt hash: " + element.ValueKind + ".");
            }
        }
    }
}

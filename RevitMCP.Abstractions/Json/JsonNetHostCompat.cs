#nullable enable
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;

namespace RevitMCP.Abstractions.Json
{
    /// <summary>
    /// Serializes Json.NET tokens without directly calling JToken overloads
    /// that may be absent when another Revit add-in has loaded an older
    /// Newtonsoft.Json assembly into the host AppDomain.
    /// </summary>
    public static class JsonNetHostCompat
    {
        public static string ToCompactJson(object? value)
        {
            var payload = value is JToken token ? ToPlainObject(token) : value;
            return JsonConvert.SerializeObject(payload);
        }

        public static object? ToPlainObject(JToken? token)
        {
            if (token == null || token.Type == JTokenType.Null || token.Type == JTokenType.Undefined)
                return null;

            if (token is JValue value)
                return value.Value;

            if (token is JObject obj)
            {
                var result = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var property in obj.Properties())
                    result[property.Name] = ToPlainObject(property.Value);
                return result;
            }

            if (token is JArray array)
            {
                var result = new List<object?>(array.Count);
                foreach (var item in array)
                    result.Add(ToPlainObject(item));
                return result;
            }

            return null;
        }
    }
}

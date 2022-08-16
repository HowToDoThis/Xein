using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;

namespace Xein
{
    public static class Extensions
    {
        #region Json
        private static JsonSerializerOptions _jso;
        private static JsonSerializerOptions DefaultJsonOptions()
        {
            if (_jso is null)
                _jso = new() { WriteIndented = true, Encoder = JavaScriptEncoder.Create(UnicodeRanges.All) };
            return _jso;
        }

        private static JsonSerializerOptions _jso2;
        private static JsonSerializerOptions JsonNoIntendedOptions()
        {
            if (_jso2 is null)
                _jso2 = new() { Encoder = JavaScriptEncoder.Create(UnicodeRanges.All) };
            return _jso2;
        }

        public static string JsonGet<T>(this T type, bool prettier = false) => JsonSerializer.Serialize(type, prettier ? DefaultJsonOptions() : JsonNoIntendedOptions());
        public static void JsonGetFile<T>(this T type, string fileName, bool prettier = false) => File.WriteAllText(fileName, type.JsonGet(prettier));
        public static T JsonObject<T>(this T type, string json) => JsonSerializer.Deserialize<T>(json);
        public static T JsonObjectFile<T>(this T type, string json) => type.JsonObject(File.ReadAllText(json));
        
        public static T JsonObject<T>(this string data) => JsonSerializer.Deserialize<T>(data);
        public static T JsonObjectFile<T>(this string data) => JsonObject<T>(File.ReadAllText(data));
        #endregion
        
        public static void Randomize<T>(this IList<T> list)
        {
            Random rand = new();
            for (var i = list.Count; i > 1; i--)
            {
                var r = rand.Next(i + 1);
                (list[i], list[r]) = (list[r], list[i]);
            }
        }

        public static string ToString(this byte[] arr)
        {
            return Encoding.UTF8.GetString(arr);
        }

        public static byte[] ToBytes(this string arr)
        {
            return Encoding.UTF8.GetBytes(arr);
        }
    }
}

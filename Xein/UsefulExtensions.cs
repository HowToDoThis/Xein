using System;
using System.Collections.Generic;
using System.Text;

namespace Xein
{
    public static class UsefulExtensions
    {
        public static void Randomize<T>(this IList<T> list)
        {
            Random rand = new();
            for (int i = list.Count; i > 1; i--)
            {
                int r = rand.Next(i + 1);
                T value = list[r];
                list[r] = list[i];
                list[i] = value;
            }
        }

        public static string ToStr(this byte[] arr)
        {
            return Encoding.UTF8.GetString(arr);
        }

        public static byte[] ToBytes(this string arr)
        {
            return Encoding.UTF8.GetBytes(arr);
        }
    }
}

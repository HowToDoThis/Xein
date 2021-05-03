using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Xein
{
    /// <summary>
    /// Console Common Type
    /// </summary>
    public enum ConsoleType
    {
        Normal,
        Debug,
        Warn,
        Error,
    }

    /// <summary>
    /// Console Item
    /// </summary>
    public class ConsoleItem
    {
        public string Message { get; private set; } = string.Empty;
        public ConsoleType MessageType { get; private set; } = ConsoleType.Normal;

        public ConsoleItem(string msg, ConsoleType type = ConsoleType.Normal)
        {
            Message = msg;
            MessageType = type;
        }

        public void Print()
        {
            ConsoleEx.Logger.WriteLine(Message);
            ConsoleEx.Logger.Flush();

            switch (MessageType)
            {
                case ConsoleType.Normal:
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.WriteLine(Message);
                    break;
                case ConsoleType.Debug:
#if DEBUG
                    Console.ForegroundColor = ConsoleColor.Gray;
                    Console.WriteLine(Message);
#endif
                    break;
                case ConsoleType.Warn:
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine(Message);
                    break;
                case ConsoleType.Error:
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(Message);
                    break;
            }

            Console.ForegroundColor = ConsoleColor.White;
        }
    }

    /// <summary>
    /// Console Extension
    /// </summary>
    public class ConsoleEx
    {
        public static StreamWriter Logger { get; private set; } = new StreamWriter(File.Open("Console.log", FileMode.Create, FileAccess.ReadWrite), Encoding.UTF8);

        /// <summary>
        /// Get Time 
        /// </summary>
        /// <returns>Short Time String</returns>
        private static string GetTime()
        {
            return DateTime.Now.TimeOfDay.ToString(@"hh\:mm\:ss");
        }

        /// <summary>
        /// Add A Warn Message
        /// </summary>
        /// <param name="msg">Message</param>
        public static void Warn(string msg)
        {
            var item = new ConsoleItem($"[{GetTime()}] [WARN] {msg}", ConsoleType.Warn);
            item.Print();
        }

        /// <summary>
        /// Add A Error Message
        /// </summary>
        /// <param name="msg">Message</param>
        public static void Error(string msg)
        {
            var item = new ConsoleItem($"[{GetTime()}] [ERROR] {msg}", ConsoleType.Error);
            item.Print();
        }

        /// <summary>
        /// Add A Message
        /// </summary>
        /// <param name="msg">Message</param>
        public static void Log(string msg)
        {
            var item = new ConsoleItem($"[{GetTime()}] {msg}", ConsoleType.Normal);
            item.Print();
        }

        /// <summary>
        /// Add A Debug Message
        /// </summary>
        /// <param name="msg">Message</param>
        public static void Debug(string msg)
        {
            var item = new ConsoleItem($"[{GetTime()}] [DEBUG] {msg}", ConsoleType.Debug);
            item.Print();
        }
    }
}

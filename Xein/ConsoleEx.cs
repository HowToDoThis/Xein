using System;
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
        private string Message { get; }
        private ConsoleType MessageType { get; }

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
                default:
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
        public static StreamWriter Logger { get; } = new StreamWriter(File.Open("Console.log", FileMode.Create, FileAccess.ReadWrite), Encoding.UTF8);

        /// <summary>
        /// Get Time 
        /// </summary>
        /// <returns>Short Time String</returns>
        private static string GetTime() => $"{DateTime.Now.ToShortDateString()} {DateTime.Now.ToShortTimeString()}";

        /// <summary>
        /// Add A Warn Message
        /// </summary>
        /// <param name="msg">Message</param>
        public static void Warn(string msg) => new ConsoleItem($"[{GetTime()}] [WARN] {msg}", ConsoleType.Warn).Print();

        /// <summary>
        /// Add A Error Message
        /// </summary>
        /// <param name="msg">Message</param>
        public static void Error(string msg) => new ConsoleItem($"[{GetTime()}] [ERROR] {msg}", ConsoleType.Error).Print();

        /// <summary>
        /// Add A Message
        /// </summary>
        /// <param name="msg">Message</param>
        public static void Log(string msg) => new ConsoleItem($"[{GetTime()}] {msg}").Print();

        /// <summary>
        /// Add A Debug Message
        /// </summary>
        /// <param name="msg">Message</param>
        public static void Debug(string msg) => new ConsoleItem($"[{GetTime()}] [DEBUG] {msg}", ConsoleType.Debug).Print();
    }
}

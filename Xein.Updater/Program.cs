using System;
using System.IO;
using System.IO.Compression;
using System.Runtime.InteropServices;

namespace Xein.Updater
{
    class Program
    {
        [DllImport("user32.dll", CharSet = CharSet.Unicode)]
        public static extern int MessageBox(IntPtr hwnd, string message, string title, int flag);

        static void ShowMsg(string msg, string title = "Error") => MessageBox(IntPtr.Zero, msg, title, 0);

        static string update = "Update.zip";
        static void Main(string[] args)
        {
            if (args.Length >= 1 && args[0] is not null)
                update = args[0];

            if (File.Exists(update))
            {
                try
                {
                    ZipFile.ExtractToDirectory(update, "./", true);
                    ShowMsg("Update Successfully.");
                }
                catch (Exception e)
                {
                    ShowMsg($"File: {update}\nMessage:\n{e.Message}\nStack Trace:\n{e.StackTrace}", "Exception Triggered");
                }
            }
            else
            {
                ShowMsg("Update File Not Found. Exiting.");
            }
        }
    }
}

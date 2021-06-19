using System;
using System.IO;
using System.IO.Compression;
using System.Runtime.InteropServices;

#pragma warning disable IDE0060 // Remove unused parameter

namespace Xein.Updater
{
    class Program
    {
        [DllImport("user32.dll", CharSet = CharSet.Unicode)]
        public static extern int MessageBox(IntPtr hwnd, string message, string title, int flag);

        static void ShowMsg(string msg, string title = "Error")
        {
            _ = MessageBox((IntPtr)0, msg, title, 0);
        }

        static void Main(string[] args)
        {
            if (!File.Exists("Update.zip"))
            {
                ShowMsg("No Update File Found. Exiting.");
            }
            else
            {
                try
                {
                    ZipFile.ExtractToDirectory("Update.zip", "./", true);
                    ShowMsg("Update Successfully.");
                }
                catch (Exception e)
                {
                    ShowMsg($"Message:\n{e.Message}\nStack Trace:\n{e.StackTrace}", "Exception Triggered");
                }
            }
        }
    }
}

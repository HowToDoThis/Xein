using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;

namespace Xein.Updater
{
    class Program
    {
        [DllImport("user32.dll", CharSet = CharSet.Unicode)]
        public static extern int MessageBox(IntPtr hwnd, string message, string title, int flag);

        static void ShowMsg(string msg, string title = "Error") => MessageBox(IntPtr.Zero, msg, title, 0);

        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                ShowMsg($"Usage:\nUpdater.exe <Start Program After Launch> <zip files> ...");
                return;
            }

            var startProgram = args[0];
            var updateFiles  = args.Skip(1).ToArray();

            if (updateFiles.Length < 1)
                updateFiles = [ "Update.zip", ];

            foreach (var file in updateFiles)
            {
                if (File.Exists(file))
                {
                    try
                    {
                        ZipFile.ExtractToDirectory(file, "./", true);
                    }
                    catch (Exception e)
                    {
                        ShowMsg($"File: {file}\nMessage:\n{e.Message}\nStack Trace:\n{e.StackTrace}", "Exception Triggered");
                    }
                }
                else
                {
                    ShowMsg("Update File Not Found. Skipping.");
                }
            }

            if (!File.Exists(startProgram))
            {
                ShowMsg($"Program Not Found. Exiting...");
                return;
            }

            try
            {
                Process.Start(startProgram);
            }
            catch (Exception e)
            {
                ShowMsg($"Program: {startProgram}\nMessage:\n{e.Message}\nStack Trace:\n{e.StackTrace}", "Exception Triggered");
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Threading;

namespace Xein
{
    /// <summary>
    /// Thread Item
    /// </summary>
    public class ThreadItem
    {
        /// <summary>
        /// Thread Name
        /// </summary>
        public string Name { get; set; } = "Thread #-1";
        /// <summary>
        /// Managed Thread Id
        /// </summary>
        public int ManagedThreadId { get; internal set; }
        /// <summary>
        /// State (Pass For Function)
        /// </summary>
        public object State { get; internal set; }

        /// <summary>
        /// Thread Function
        /// </summary>
        public WaitCallback Function { get; internal set; }
        /// <summary>
        /// Running Thread
        /// </summary>
        public Thread Thread { get; set; }

        public bool Logging { get; set; } = true;

        /// <summary>
        /// Create Thread without State(Pass to function)
        /// </summary>
        /// <param name="name">Thread Name</param>
        /// <param name="func">Function</param>
        public ThreadItem(string name, WaitCallback func, bool logging = true)
        {
            Name = name;
            Function = func;
            Logging = logging;
        }

        /// <summary>
        /// Create Thread with State(Pass to function)
        /// </summary>
        /// <param name="name">Thread Name</param>
        /// <param name="func">Function</param>
        /// <param name="obj">State</param>
        public ThreadItem(string name, WaitCallback func, object obj, bool logging = true)
        {
            Name = name;
            Function = func;
            State = obj;
            Logging = logging;
        }
    }

    /// <summary>
    /// This Class Are Handling Every Single Thread Created
    /// Within This Program For Tracking Purpose
    /// And Runs with ThreadPool.QueueUserWorkItem
    /// </summary>
    public static class ThreadEx
    {
        public static List<ThreadItem> Threads { get; private set; } = new();

        private static void DummyThreadFunction(object state)
        {
            var th = Thread.CurrentThread;

            if (state is not ThreadItem item)
            {
                ConsoleEx.Error($"[Thread #{th.ManagedThreadId}] Has no ThreadItem. Returning.");
                return;
            }

            item.Thread = th;
            item.ManagedThreadId = th.ManagedThreadId;
            th.Name = item.Name;

            // Add To Lists
            Threads.Add(item);

            // Logging
            if (item.Logging)
                ConsoleEx.Debug($"[Thread #{item.ManagedThreadId}] is Ready to run [{item.Function.Method.Name}]" + (item.State is not null ? $" with [{item.State}]" : ""));

            // Start Thread/Function Time
            var startTime = DateTime.Now;

            // Start Job
            item.Function(item.State);

            if (item.Logging)
                ConsoleEx.Log($"[Thread {item.ManagedThreadId}] Function Execute Time: {(DateTime.Now - startTime).TotalMilliseconds}");

            // Since end of func, remove from list for showing invalid number 'using' threads
            Threads.Remove(item);
        }

        /// <summary>
        /// Create a Thread
        /// </summary>
        /// <returns>Thread that running with this function</returns>
        public static Thread Create(string name, WaitCallback func)
        {
            var item = new ThreadItem(name, func);
            ThreadPool.QueueUserWorkItem(new(DummyThreadFunction), item);
            return item.Thread is null ? null : item.Thread;
        }

        /// <summary>
        /// Create a Thread
        /// </summary>
        /// <returns>Thread that running with this function</returns>
        public static Thread Create(string name, WaitCallback func, object state)
        {
            var item = new ThreadItem(name, func, state);
            ThreadPool.QueueUserWorkItem(new(DummyThreadFunction), item);
            return item.Thread is null ? null : item.Thread;
        }
    }
}

using System;
using System.Net;

namespace Xein.Net
{
    public class HttpServer
    {
        public delegate void ExecuteFunction(HttpListenerRequest request, HttpListenerResponse response);

        public readonly HttpListener Listener;
        public bool IsStarted;

        public ExecuteFunction OnReceiveContext;

        public HttpServer(int port, bool isLocal = false)
        {
            Listener = new();
            Listener.Prefixes.Add($"http://{(isLocal ? "localhost" : "+")}:{port}/");
        }

        public void Start()
        {
            if (IsStarted)
                return;

            IsStarted = true;
            Listener.Start();
            Listener.BeginGetContext(HttpServer_Callback, null);
        }

        public void Stop()
        {
            if (!IsStarted)
                return;

            IsStarted = false;
            Listener.Stop();
        }

        private void HttpServer_Callback(IAsyncResult result)
        {
            if (!IsStarted)
                return;

            try
            {
                var context = Listener.EndGetContext(result);
                Listener.BeginGetContext(HttpServer_Callback, null);

                var request = context.Request;
                var response = context.Response;

                OnReceiveContext?.Invoke(request, response);
            }
            catch (Exception e)
            {
                Console.WriteLine($"[HttpServer] Exception Found: {e.Message}\n{e.StackTrace}");
            }
        }
    }
}

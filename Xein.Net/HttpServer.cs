using System;
using System.Net;

namespace Xein.Net
{
    public class HttpServer
    {
        public delegate void ExecuteFunction(HttpListenerRequest request, HttpListenerResponse response);

        private readonly HttpListener listener;
        private bool isStarted;

        public ExecuteFunction OnReceiveContext;

        public HttpServer(int port, bool isLocal = false)
        {
            listener = new();
            listener.Prefixes.Add($"http://{(isLocal ? "localhost" : "+")}:{port}/");
        }

        public void Start()
        {
            if (isStarted)
                return;

            isStarted = true;
            listener.Start();
            listener.BeginGetContext(HttpServer_Callback, null);
        }

        public void Stop()
        {
            if (!isStarted)
                return;

            isStarted = false;
            listener.Stop();
        }

        private void HttpServer_Callback(IAsyncResult result)
        {
            if (!isStarted)
                return;

            try
            {
                var context = listener.EndGetContext(result);
                listener.BeginGetContext(HttpServer_Callback, null);

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

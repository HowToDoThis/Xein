using System;
using System.Net;
using System.Net.Sockets;

namespace Xein.Net
{
    public static partial class Extension
    {
        public static bool IsEmpty(this string s) => string.IsNullOrWhiteSpace(s) || string.IsNullOrEmpty(s);

        public static string GetRemoteIP(this Socket s)
        {
            var str = (s.RemoteEndPoint as IPEndPoint)?.Address.ToString();
            return str.IsEmpty() ? "INVALID" : str[(str.LastIndexOf(':') + 1)..];
        }
        public static int GetRemotePort(this Socket s) => (s.RemoteEndPoint as IPEndPoint)?.Port ?? -1;
    }

    public class SocketEx
    {
        private Socket Socket { get; }
        public DateTime ConnectedTime { get; } = DateTime.Now;

        public int TotalReceived { get; private set; }
        public int TotalSent { get; private set; }

        public string Ip   { get; }
        public int    Port { get; }

        public SocketEx(Socket socket)
        {
            Socket = socket;
            ConnectedTime = DateTime.Now;

            Ip   = Socket.GetRemoteIP();
            Port = Socket.GetRemotePort();
        }

        public void SetNoDelay(bool noDelay = true)
        {
            Socket.NoDelay = noDelay;
        }

        private bool bAsyncMethod;
        public void UseAsync()
        {
            if (bAsyncMethod)
            {
                bAsyncMethod = false;
            }
            else
            {
                bAsyncMethod = true;
            }
        }

        #region GetXXX
        public Socket GetSocket() => Socket;
        public long GetAvailable() => Socket.Available;
        #endregion
        
        /// <summary>
        /// Sent data to End Socket
        /// </summary>
        public int Send(byte[] data)
        {
            try
            {
                var ret = Socket.Send(data);
                TotalSent += ret;

                return ret;
            }
            catch (SocketException e)
            {
                return e.ErrorCode;
            }
        }
        
        /// <summary>
        /// Read data to buffer
        /// </summary>
        /// <returns>received bytes, negative equals to Socket.ErrorCode</returns>
        public int Read(byte[] data)
        {
            try
            {
                var ret = Socket.Receive(data);
                TotalReceived += ret;

                return ret;
            }
            catch (SocketException e)
            {
                return -e.ErrorCode;
            }
        }

        /// <summary>
        /// Close Socket and Dispose
        /// </summary>
        public void Dispose()
        {
            Socket?.Dispose();
        }
    }
}

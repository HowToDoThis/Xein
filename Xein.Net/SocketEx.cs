using System;
using System.Net;
using System.Net.Sockets;

namespace Xein.Net
{
    public class SocketEx
    {
        private Socket Socket { get; }
        public DateTime ConnectedTime { get; }
        public Guid Guid { get; } = Guid.NewGuid();

        public string Ip { get; }
        public int Port => ((IPEndPoint)Socket.RemoteEndPoint)!.Port;
        public int TotalReceived { get; private set; }
        public int TotalSent { get; private set; }

        /// <summary>
        /// Socket Extension
        /// </summary>
        public SocketEx(Socket socket)
        {
            Socket = socket;
            ConnectedTime = DateTime.Now;

            Ip = ((IPEndPoint)Socket.RemoteEndPoint)?.Address.ToString();
            Ip = Ip?[(Ip.LastIndexOf(':') + 1)..];
        }

        #region IsXXX
        public bool IsStillAlive()
        {
            return !Socket.Poll(1000, SelectMode.SelectRead) || (Socket.Available != 0 || IsSocketStillAlive());
        }

        private bool IsSocketStillAlive()
        {
            try
            {
                Socket.Send(new byte[] {0});
                return true;
            }
            catch (SocketException e)
            {
                return e.SocketErrorCode is SocketError.WouldBlock or SocketError.Success;
            }
            catch (ObjectDisposedException)
            { }

            return false;
        }
        #endregion

        #region GetXXX
        public Socket GetSocket() => Socket;
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

using System;
using System.Net;
using System.Net.Sockets;

#pragma warning disable CS8794 // The input always matches the provided pattern.

namespace Xein.Net
{
    public class SocketEx
    {
        public Socket Socket { get; set; } = null;
        public DateTime ConnectedTime { get; set; }
        public Guid GUID { get; private set; } = Guid.NewGuid();

        public string IP { get; set; } = string.Empty;
        public int TotalReceived { get; set; } = 0;
        public int TotalSent { get; set; } = 0;

        public byte[] buffer = new byte[short.MaxValue];
        public int bufRead = 0;

        /// <summary>
        /// Socket Extension
        /// </summary>
        public SocketEx(Socket socket)
        {
            Socket = socket;
            ConnectedTime = DateTime.Now;

            IP = ((IPEndPoint)Socket.RemoteEndPoint).Address.ToString();
            IP = IP[(IP.LastIndexOf(':') + 1)..];
        }

        /// <summary>
        /// Sent data to End Socket
        /// </summary>
        public int Send(byte[] data)
        {
            try
            {
                int ret = Socket.Send(data);
                TotalSent += ret;

                return ret;
            }
            catch (SocketException e)
            {
                return e.ErrorCode;
            }
        }

        public bool IsStillAlive()
        {
            if (Socket.Poll(1000, SelectMode.SelectRead))
                if (Socket.Available == 0)
                    if (!IsSocketStillAlive())
                        return false;

            return true;
        }

        public bool IsSocketStillAlive()
        {
            try
            {
                byte[] nul = new byte[1];
                nul[0] = 0;
                Socket.Send(nul);

                return true;
            }
            catch (SocketException e)
            {
                if (e.SocketErrorCode is not SocketError.WouldBlock or not SocketError.Success)
                    return false;
            }
            catch (ObjectDisposedException)
            { }

            return false;
        }

        /// <summary>
        /// Read data to buffer
        /// </summary>
        public int Read()
        {
            ResetRead();

            try
            {
                int ret = Socket.Receive(buffer);
                bufRead = ret;
                TotalReceived += ret;

                return ret;
            }
            catch (SocketException e)
            {
                return e.ErrorCode;
            }
        }

        /// <summary>
        /// Reset buffer
        /// </summary>
        public void ResetRead()
        {
            Array.Clear(buffer, 0, buffer.Length);
            bufRead = 0;
        }

        /// <summary>
        /// Close Connection
        /// </summary>
        public void Shutdown()
        {
            Socket.Shutdown(SocketShutdown.Both);
        }

        /// <summary>
        /// Close Socket and Dispose
        /// </summary>
        public void Dispose()
        {
            Shutdown();
            Socket.Dispose();
        }
    }
}

using System;
using System.Net;
using System.Net.Sockets;

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
                return e.SocketErrorCode is SocketError.WouldBlock or SocketError.Success;
            }
            catch (ObjectDisposedException)
            { }

            return false;
        }

        /// <summary>
        /// Read data to buffer
        /// </summary>
        /// <returns>received bytes, negative equals to Socket.ErrorCode</returns>
        public int Read(byte[] data)
        {
            try
            {
                int ret = Socket.Receive(data);
                TotalReceived += ret;

                return ret;
            }
            catch (SocketException e)
            {
                return -e.ErrorCode;
            }
        }

        /// <summary>
        /// Close Connection
        /// </summary>
        public void Shutdown()
        {
            if (Socket is not null)
                Socket.Shutdown(SocketShutdown.Both);
        }

        /// <summary>
        /// Close Socket and Dispose
        /// </summary>
        public void Dispose()
        {
            if (Socket is not null)
            {
                Shutdown();
                Socket.Dispose();
            }
        }
    }
}

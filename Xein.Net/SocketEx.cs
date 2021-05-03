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

        /// <summary>
        /// Read data to buffer
        /// </summary>
        public int Read()
        {
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
    }
}

using System;
using System.Net.Sockets;

namespace Xein.Interfaces
{
    public interface ISocket
    {
        // Properties
        public string IP { get; }
        public DateTime ConnectedTime { get; }
        public Socket Socket { get; }

        // Socket Data
        public long TotalReceived { get; }
        public long TotalSented { get; }

        // Methods
        public int Read(byte[] buf);
        public int Send(byte[] buf);
    }
}

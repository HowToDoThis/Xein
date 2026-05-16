using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Xein.Net
{
    public class SocketEventArgs : EventArgs
    {
        public int Port { get; }
        public SocketEx Client { get; }
        public SocketError Error { get; }

        public SocketEventArgs(int port, SocketEx client, SocketError error)
        {
            Port   = port;
            Client = client;
            Error  = error;
        }
    }

    public class SocketReceivedEventArgs : EventArgs
    {
        public byte[] Buffer { get; }
        public int    Count  { get; }

        public SocketReceivedEventArgs(byte[] buffer, int count)
        {
            Buffer = buffer;
            Count  = count;
        }
    }

    public class ExceptionEventArgs : EventArgs
    {
        public Exception Exception { get; }
        public string Message { get; }

        public ExceptionEventArgs(Exception exception, string message)
        {
            Exception = exception;
            Message   = message;
        }
    }

    public class ListenerInfo
    {
        public int    ID     { get; init; }
        public int    Port   { get; init; }
        public Socket Socket { get; init; }
    }

}

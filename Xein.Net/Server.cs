using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Xein.Net
{
    public class Server
    {
        public Socket tcpSocket = null;
        public Socket udpSocket = null;

        public bool IsServerInit { get; } = false;
        public bool IsServerClosed { get; } = false;

        public List<SocketEx> Clients { get; set; } = new();

        /// <summary>
        /// Xein's Server Module
        /// </summary>
        /// <param name="ip">The IP should running on</param>
        /// <param name="tcpPort">TcpServer Port</param>
        /// <param name="udpPort">UdpServer Port</param>
        /// <param name="initTcp">Init TcpServer</param>
        /// <param name="initUdp">Init UdpServer</param>
        public Server(IPAddress ip, int tcpPort, int udpPort, bool initTcp = true, bool initUdp = true)
        {
            if (initTcp == false && initUdp == false)
                throw new InvalidOperationException("Pick atleast 1 socket stream type!");

            if (initTcp)
            {
                try
                {
                    tcpSocket = new(SocketType.Stream, ProtocolType.Tcp)
                    {
                        NoDelay = true,
                        DualMode = true,
                    };

                    tcpSocket.Bind(new IPEndPoint(ip, tcpPort));
                }
                catch
                {
                    throw new Exception("TcpSocket failed to init");
                }
            }

            if (initUdp)
            {
                try
                {
                    udpSocket = new(SocketType.Dgram, ProtocolType.Udp)
                    {
                        DualMode = true,
                    };

                    udpSocket.Bind(new IPEndPoint(ip, udpPort));
                }
                catch
                {
                    throw new Exception("UdpSocket failed to init");
                }

            }

            if (tcpSocket is not null)
            {
                if (tcpSocket.IsBound == false)
                    throw new InvalidProgramException("TcpServer Init Successfully, but is not bounded?");
            }

            if (udpSocket is not null)
            {
                if (udpSocket.IsBound == false)
                    throw new InvalidProgramException("UdpServer Init Successfully, but is not bounded?");
            }

            IsServerInit = true;
        }

        /// <summary>
        /// Call Socket Ready To Accpet Clients
        /// </summary>
        public void Listen(int backlog = int.MaxValue)
        {
            if (!IsServerInit)
                throw new InvalidOperationException("This should not be happen, Please Check Init");

            if (tcpSocket is null)
                throw new InvalidOperationException("TcpSocket Is Null? Please Check Init");

            tcpSocket.Listen(backlog);

            ThreadPool.QueueUserWorkItem(AcceptClient);
            ThreadPool.QueueUserWorkItem(ListenClient);
        }

        // STARTING HERE IS PRIVATE METHODS

        private readonly ManualResetEvent tcpAccept = new(false);
        private void AcceptClient(object state)
        {
            while (IsServerInit)
            {
                try
                {
                    tcpAccept.Reset();
                    tcpSocket.BeginAccept(new AsyncCallback(Server_BeginAccept), tcpSocket);
                    tcpAccept.WaitOne();
                }
                catch (ObjectDisposedException)
                {
                    if (!IsServerClosed)
                        break;
                }
            }
        }

        private void Server_BeginAccept(IAsyncResult ar)
        {
            try
            {
                var soc = ar.AsyncState as Socket;
                var end = soc.EndAccept(ar);
                var ex = new SocketEx(end);

                Clients.Add(ex);

                Console.WriteLine($"[Server] Client '{ex.IP}' has connected!");
            }
            catch
            {
                // i hate exceptions
            }

            tcpAccept.Set();
        }

        private void ListenClient(object state)
        {
            while (IsServerInit)
            {
                if (Clients.Count <= 0)
                {
                    Thread.Sleep(100);
                    continue;
                }

                // copy a list for preventing shit problems
                foreach (var client in Clients.ToList())
                {
                    // TODO: Do a better check here pls
                    if (client.Socket.Available == 0)
                        continue;

                    // for temp, print all data to here
                    client.ResetRead();
                    var readed = client.Read();

                    Console.WriteLine($"[Client '{client.IP}'] sent {readed} bytes: {Encoding.UTF8.GetString(client.buffer)}");
                }
            }
        }
    }
}

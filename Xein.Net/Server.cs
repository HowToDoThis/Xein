using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Xein.Net
{
    public class ClientStatsEventArgs : EventArgs
    {
        public SocketEx Client { get; set; }

        public string IP { get; set; }
        public DateTime Time { get; set; }
    }

    public class ShutdownEventArgs : EventArgs
    {
        public bool IsTcpAcceptBusying { get; set; }
        public bool IsTcpBusying { get; set; }
        public bool IsUdpBusying { get; set; }
    }

    public class ReceivedDataEventArgs : EventArgs
    {
        public byte[] ReceivedData { get; set; }
        public int ReceivedSize { get; set; }

        public EndPoint EndPoint { get; set; }
    }

    public class Server
    {
        #region Properties/Fields

        public Socket tcpSocket = null;
        public Socket udpSocket = null;

        public List<SocketEx> Clients { get; set; } = new();

        private bool tryShutdown = false;
        private bool isTcpAcceptBusy;
        private bool isTcpBusy;
        private bool isUdpBusy;
        #endregion

        #region Events
        public event EventHandler<ClientStatsEventArgs> ClientConnected;
        public event EventHandler<ClientStatsEventArgs> ClientDisconnected;

        public event EventHandler<ReceivedDataEventArgs> ReceivedTcpClientData;
        public event EventHandler<ReceivedDataEventArgs> ReceivedUdpClientData;

        public event EventHandler<ShutdownEventArgs> ShutdownCalled;
        public event EventHandler Running;
        #endregion

        /// <summary>
        /// Xein's Server Module
        /// </summary>
        public Server()
        {

        }

        /// <summary>
        /// Setup TcpSocket
        /// </summary>
        /// <returns>Is TcpSocket Bounded</returns>
        public bool SetupTcp(IPAddress ip, int port)
        {
            try
            {
                tcpSocket = new(SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true,
                    DualMode = true,
                };

                tcpSocket.Bind(new IPEndPoint(ip, port));
            }
            catch (Exception e)
            {
                throw new Exception($"TcpSocket failed to init\nMessage: {e.Message}");
            }

            return tcpSocket.IsBound;
        }

        /// <summary>
        /// Setup UdpSocket
        /// </summary>
        /// <returns>Is UdpSocket Bounded</returns>
        public bool SetupUdp(IPAddress ip, int port)
        {
            try
            {
                udpSocket = new(SocketType.Dgram, ProtocolType.Udp)
                {
                    DualMode = true,
                };

                udpSocket.Bind(new IPEndPoint(ip, port));
            }
            catch (Exception e)
            {
                throw new Exception($"UdpSocket failed to init\nMessage: {e.Message}");
            }

            return udpSocket.IsBound;
        }

        /// <summary>
        /// Call TcpSocket Ready To Accept Clients
        /// </summary>
        public void Start(bool udp = false, int backlog = int.MaxValue)
        {
            if (tcpSocket is null)
                throw new InvalidOperationException("TcpSocket is null? Did you call SetupTcp()?");

            tcpSocket.Listen(backlog);

            ThreadPool.QueueUserWorkItem(AcceptClient);
            ThreadPool.QueueUserWorkItem(ListenClient);

            if (udp && udpSocket is null)
                throw new InvalidOperationException("UdpSocket is null? Did you call SetupUdp()?");
            else
                ThreadPool.QueueUserWorkItem(ReceiveFromUdp);

            Running?.Invoke(this, new());
        }

        public void Shutdown()
        {
            tryShutdown = true;

            ShutdownCalled?.Invoke(this, new() { IsTcpAcceptBusying = isTcpAcceptBusy, IsTcpBusying = isTcpBusy, IsUdpBusying = isUdpBusy });

        wait:
            Console.WriteLine($"[Server Shutdown] TcpClient: {Clients.Count} isTcpAcceptBusy: {isTcpAcceptBusy} isTcpBusy: {isTcpBusy} isUdpBusy: {isUdpBusy}");

            if (Clients.Count >= 1)
            {
                foreach (SocketEx client in Clients.ToList())
                {
                    client.Dispose();
                    Clients.Remove(client);
                }
            }

            Thread.Sleep(1000);

            if (isTcpAcceptBusy)
                goto wait;
            if (isTcpBusy)
                goto wait;
            if (isUdpBusy)
                goto wait;
        }

        public void Dispose()
        {
            if (tcpSocket is not null)
                tcpSocket.Dispose();
            if (udpSocket is not null)
                udpSocket.Dispose();

            foreach (var client in Clients.ToList())
                client.Dispose();
            Clients.Clear();
        }

        #region Private Methods

        private readonly ManualResetEvent tcpAccept = new(false);
        private void AcceptClient(object state)
        {
            isTcpAcceptBusy = true;

            while (!tryShutdown)
            {
                try
                {
                    tcpAccept.Reset();
                    tcpSocket.BeginAccept(new AsyncCallback(Server_BeginAccept), tcpSocket);
                    tcpAccept.WaitOne();
                }
                catch (ObjectDisposedException)
                {
                    if (tryShutdown)
                        break;
                }
            }

            isTcpAcceptBusy = false;
        }

        private void Server_BeginAccept(IAsyncResult ar)
        {
            try
            {
                var soc = ar.AsyncState as Socket;
                var end = soc.EndAccept(ar);
                var ex = new SocketEx(end);

                Clients.Add(ex);

                ClientConnected?.Invoke(this, new() { Client = ex, IP = ex.IP, Time = ex.ConnectedTime });
            }
            catch
            {
                // i hate exceptions
            }

            tcpAccept.Set();
        }

        private void ListenClient(object state)
        {
            isTcpBusy = true;

            while (!tryShutdown)
            {
                if (tryShutdown)
                    break;

                if (Clients.Count <= 0)
                {
                    Thread.Sleep(100);
                    continue;
                }

                foreach (var client in Clients.ToList())
                {
                    if (!client.IsStillAlive())
                    {
                        ClientDisconnected?.Invoke(this, new() { Client = client, IP = client.IP, Time = client.ConnectedTime });
                        Clients.Remove(client);
                        continue;
                    }

                    var readed = client.Read();
                    ReceivedTcpClientData?.Invoke(this, new() { EndPoint = client.Socket.RemoteEndPoint, ReceivedData = client.buffer.ToArray(), ReceivedSize = readed });
                }
            }

            isTcpBusy = false;
        }

        private void ReceiveFromUdp(object state)
        {
            isUdpBusy = true;

            while (!tryShutdown)
            {
                if (tryShutdown)
                    break;

                byte[] data = new byte[short.MaxValue];
                IPEndPoint udpEndPoint = new(IPAddress.Any, 0);
                EndPoint ep = udpEndPoint;

                var recv = udpSocket.ReceiveFrom(data, ref ep);
                ReceivedUdpClientData?.Invoke(this, new() { EndPoint = ep, ReceivedData = data, ReceivedSize = recv });
            }

            isUdpBusy = false;
        }

        #endregion
    }
}

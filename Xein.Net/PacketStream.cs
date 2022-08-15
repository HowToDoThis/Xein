using System;
using System.IO;
using System.Text;

namespace Xein.Net
{
    // extensions
    public static class NetworkExtensions
    {
        public static byte[] Reverse(this byte[] bytes, bool isLe = true)
        {
            if (!isLe)
                Array.Reverse(bytes);
            return bytes;
        }
    }

    public class PacketStream
    {
        /// <summary>
        /// Internal Stream
        /// </summary>
        public MemoryStream Stream { get; private set; }

        public int StreamLength { get; private set; }

        /// <summary>
        /// Create a Stream
        /// </summary>
        public PacketStream()
        {
            Stream = new();
        }

        /// <summary>
        /// Load Data To Stream
        /// </summary>
        public PacketStream(byte[] data)
        {
            Stream = new(data, false);
            StreamLength = data.Length;
        }

        #region Read
        public string ReadStringA(int size = 0)
        {
            var buf = string.Empty;
            if (size == 0)
            {
                var cur = Convert.ToChar(ReadInt8());
                while (cur != '\0')
                {
                    buf += cur;
                    cur = Convert.ToChar(ReadInt8());
                }
            }
            else
            {
                for (var i = 0; i < size; i++)
                    buf += Convert.ToChar(ReadInt8());
            }
            return buf;
        }

        public string ReadStringW(int size)
        {
            var buf = string.Empty;
            for (var i = 0; i < size; i++)
                buf += Convert.ToChar(ReadUInt16());
            return buf;
        }

        public byte[] ReadSize(int size)
        {
            if (StreamLength - Stream.Position < size)
                return Array.Empty<byte>();
            var buf = new byte[size];
            if (Stream.Read(buf) != size)
                buf = Array.Empty<byte>();
            return buf;
        }

        public byte ReadInt8() => StreamLength - Stream.Position < sizeof(byte) ? (byte)0 : Convert.ToByte(Stream.ReadByte());
        public short ReadInt16(bool isLe = true) => StreamLength - Stream.Position < sizeof(short) ? (short)0 : BitConverter.ToInt16(Read<short>().Reverse(isLe));
        public ushort ReadUInt16(bool isLe = true) => StreamLength - Stream.Position < sizeof(ushort) ? (ushort)0 : BitConverter.ToUInt16(Read<ushort>().Reverse(isLe));
        public int ReadInt24(bool isLe = true) => StreamLength - Stream.Position < 3 ? 0 : BitConverter.ToInt32(isLe ? new byte[] { ReadInt8(), ReadInt8(), ReadInt8(), 0 } : new byte[] { 0, ReadInt8(), ReadInt8(), ReadInt8() });
        public int ReadInt32(bool isLe = true) => StreamLength - Stream.Position < sizeof(int) ? 0 : BitConverter.ToInt32(Read<int>().Reverse(isLe));
        public uint ReadUInt32(bool isLe = true) => StreamLength - Stream.Position < sizeof(uint) ? 0 : BitConverter.ToUInt32(Read<uint>().Reverse(isLe));
        public long ReadInt64(bool isLe = true) => StreamLength - Stream.Position < sizeof(long) ? 0 : BitConverter.ToInt64(Read<long>().Reverse(isLe));
        public ulong ReadUInt64(bool isLe = true) => StreamLength - Stream.Position < sizeof(ulong) ? 0 : BitConverter.ToUInt64(Read<ulong>().Reverse(isLe));
        public float ReadFloat(bool isLe = true) => StreamLength - Stream.Position < sizeof(float) ? 0 : BitConverter.ToSingle(Read<float>().Reverse(isLe));
        public double ReadDouble(bool isLe = true) => StreamLength - Stream.Position < sizeof(double) ? 0 : BitConverter.ToDouble(Read<double>().Reverse(isLe));

        private unsafe byte[] Read<T>() where T : unmanaged
        {
            var buf = new byte[sizeof(T)];
            if (Stream.Read(buf) != sizeof(T))
                buf = Array.Empty<byte>();
            return buf;
        }
        #endregion

        #region Write
        public void Write(byte[] data) => Stream.Write(data);

        public void Write(byte data) => Stream.WriteByte(data);
        public void Write(bool data) => Stream.WriteByte(Convert.ToByte(data));
        public void Write(Enum data) => Stream.WriteByte(Convert.ToByte(data));

        public void WriteInt8(int data) => Stream.WriteByte((byte)data);
        public void WriteInt16(int data, bool isLe = true) => Stream.Write(BitConverter.GetBytes((short)data));
        public void WriteUInt16(int data) => Stream.Write(BitConverter.GetBytes((ushort)data));
        public void WriteInt24(int data) => Write(BitConverter.GetBytes(data)[..2]);
        public void WriteInt32(int data) => Stream.Write(BitConverter.GetBytes(data));
        public void WriteUInt32(uint data) => Stream.Write(BitConverter.GetBytes(data));
        public void WriteInt64(long data) => Stream.Write(BitConverter.GetBytes(data));
        public void WriteUInt64(ulong data) => Stream.Write(BitConverter.GetBytes(data));
        public void WriteFloat(float data) => Stream.Write(BitConverter.GetBytes(data));
        public void WriteDouble(double data) => Stream.Write(BitConverter.GetBytes(data));

        public void WriteStringA(string data)
        {
            var test = Encoding.UTF8.GetBytes(data);
            Write(test);
        }

        public void WriteStringW(string data)
        {
            var test = Encoding.Unicode.GetBytes(data);
            Write(test);
        }
        #endregion

        public byte[] GetLeftover() => ReadSize(StreamLength - (int)GetPos());
        public byte[] GetBuffer() => Stream.GetBuffer();
        public byte[] GetData() => Stream.ToArray();
        public long GetPos() => Stream.Position;

        public void SetPos(long pos)
        {
            Stream.Position = pos;
        }
    }
}

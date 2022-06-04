using System;
using System.IO;
using System.Text;

namespace Xein.Net
{
    public class PacketStream
    {
        /// <summary>
        /// Internal Stream
        /// </summary>
        public MemoryStream Stream { get; private set; }

        public int StreamLength { get; private set; } = 0;

        /// <summary>
        /// Create a Stream
        /// </summary>
        public PacketStream()
        {
            Stream = new MemoryStream();
        }

        /// <summary>
        /// Load Data To Stream
        /// </summary>
        public PacketStream(byte[] data)
        {
            Stream = new MemoryStream(data, false);
            StreamLength = data.Length;
        }

        #region Read
        public string ReadStringA(int size = 0)
        {
            string buf = string.Empty;
            if (size == 0)
            {
                char cur = Convert.ToChar(ReadInt8());
                while (cur != '\0')
                {
                    buf += cur;
                    cur = Convert.ToChar(ReadInt8());
                }
            }
            else
            {
                for (int i = 0; i < size; i++)
                    buf += Convert.ToChar(ReadInt8());
            }
            return buf;
        }

        public string ReadStringW(int size)
        {
            string buf = string.Empty;
            for (int i = 0; i < size; i++)
                buf += Convert.ToChar(ReadUInt16());
            return buf;
        }

        public byte[] ReadSize(int size)
        {
            if (StreamLength - Stream.Position < size)
                return new byte[] { 0 };

            byte[] buf = new byte[size];
            Stream.Read(buf);
            return buf;
        }

        public void Read(byte[] data)
        {
            Stream.Read(data);
        }

        public byte ReadInt8()
        {
            return StreamLength - Stream.Position < sizeof(byte) ? (byte)0 : Convert.ToByte(Stream.ReadByte());
        }

        public short ReadInt16()
        {
            return StreamLength - Stream.Position < sizeof(short) ? (short)0 : BitConverter.ToInt16(Read<short>());
        }

        public ushort ReadUInt16()
        {
            return StreamLength - Stream.Position < sizeof(ushort) ? (ushort)0 : BitConverter.ToUInt16(Read<ushort>());
        }

        public int ReadInt24()
        {
            return StreamLength - Stream.Position < 3 ? 0 : BitConverter.ToInt32(new byte[] { ReadInt8(), ReadInt8(), ReadInt8(), 0 });
        }

        public int ReadInt32()
        {
            return StreamLength - Stream.Position < sizeof(int) ? 0 : BitConverter.ToInt32(Read<int>());
        }

        public uint ReadUInt32()
        {
            return StreamLength - Stream.Position < sizeof(uint) ? 0 : BitConverter.ToUInt32(Read<uint>());
        }

        public long ReadInt64()
        {
            return StreamLength - Stream.Position < sizeof(long) ? 0 : BitConverter.ToInt64(Read<long>());
        }

        public ulong ReadUInt64()
        {
            return StreamLength - Stream.Position < sizeof(ulong) ? 0 : BitConverter.ToUInt64(Read<ulong>());
        }

        public float ReadFloat()
        {
            return StreamLength - Stream.Position < sizeof(float) ? 0 : BitConverter.ToSingle(Read<float>());
        }

        public double ReadDouble()
        {
            return StreamLength - Stream.Position < sizeof(double) ? 0 : BitConverter.ToDouble(Read<double>());
        }

        private unsafe byte[] Read<T>() where T : unmanaged
        {
            byte[] buf = new byte[sizeof(T)];
            Stream.Read(buf);
            return buf;
        }
        #endregion

        #region Write
        public void Write(byte[] data) => Stream.Write(data);

        public void Write(bool data) => Stream.WriteByte(Convert.ToByte(data));
        public void Write(Enum data) => Stream.WriteByte(Convert.ToByte(data));

        public void WriteInt8(int data) => Stream.WriteByte((byte)data);

        public void WriteInt16(int data, bool isLE = true) => Stream.Write(BitConverter.GetBytes((short)data));
        public void WriteUInt16(int data) => Stream.Write(BitConverter.GetBytes((ushort)data));

        public void WriteInt24(int data) => Write(BitConverter.GetBytes(data)[..2]);

        public void WriteInt32(int data) => Stream.Write(BitConverter.GetBytes(data));
        public void WriteUInt32(uint data) => Stream.Write(BitConverter.GetBytes(data));

        public void WriteInt64(long data) => Stream.Write(BitConverter.GetBytes(data));
        public void WriteUInt64(ulong data) => Stream.Write(BitConverter.GetBytes(data));

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

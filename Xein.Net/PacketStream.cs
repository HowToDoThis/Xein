using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Xein.Net
{
    public class PacketStream
    {
        public MemoryStream Stream { get; private set; }

        public int StreamLength { get; private set; } = 0;


        /// <summary>
        /// Create a 32KB Stream
        /// </summary>
        public PacketStream()
        {
            Stream = new MemoryStream(short.MaxValue);
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

        public string ReadString()
        {
            string buf = string.Empty;

            char cur = Convert.ToChar(ReadByte());
            while (cur != '\0')
            {
                buf += cur.ToString();
                cur = Convert.ToChar(ReadByte());
            }

            return buf;
        }

        public byte[] ReadSize(int size)
        {
            if (StreamLength - Stream.Position < size)
                return null;

            byte[] buf = new byte[size];
            Stream.Read(buf);
            return buf;
        }

        public void Read(byte[] data)
        {
            Stream.Read(data);
        }

        public byte ReadByte()
        {
            return StreamLength - Stream.Position < sizeof(byte) ? (byte)0 : Convert.ToByte(Stream.ReadByte());
        }

        public short ReadShort()
        {
            return StreamLength - Stream.Position < sizeof(short) ? (short)0 : BitConverter.ToInt16(Read<short>());
        }

        public ushort ReadUShort()
        {
            return StreamLength - Stream.Position < sizeof(ushort) ? (ushort)0 : BitConverter.ToUInt16(Read<ushort>());
        }

        public int ReadInt24()
        {
            return StreamLength - Stream.Position < 3 ? 0 : BitConverter.ToInt32(new byte[] { ReadByte(), ReadByte(), ReadByte(), 0 });
        }

        public int ReadInt()
        {
            return StreamLength - Stream.Position < sizeof(int) ? 0 : BitConverter.ToInt32(Read<int>());
        }

        public uint ReadUInt()
        {
            return StreamLength - Stream.Position < sizeof(uint) ? 0 : BitConverter.ToUInt32(Read<uint>());
        }

        public long ReadLong()
        {
            return StreamLength - Stream.Position < sizeof(long) ? 0 : BitConverter.ToInt64(Read<long>());
        }

        public ulong ReadULong()
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

        private byte[] Read<T>() where T : unmanaged
        {
            unsafe
            {
                byte[] buf = new byte[sizeof(T)];
                Stream.Read(buf);
                return buf;
            }
        }

        #endregion

        #region Write

        public void Write(byte[] data)
        {
            Stream.Write(data);
        }

        public void WriteByte(int data) => Write((byte)data);
        public void Write(byte data)
        {
            Stream.WriteByte(data);
        }

        public void Write(bool data)
        {
            Stream.WriteByte(Convert.ToByte(data));
        }

        public void WriteInt16(int data) => Write((short)data);
        public void Write(short data)
        {
            Stream.Write(BitConverter.GetBytes(data));
        }

        public void WriteUInt16(int data) => Write((ushort)data);
        public void Write(ushort data)
        {
            Stream.Write(BitConverter.GetBytes(data));
        }

        public void WriteInt24(int data) => Write(BitConverter.GetBytes(data)[..2]);

        public void WriteInt32(int data) => Write(data);
        public void Write(int data)
        {
            Stream.Write(BitConverter.GetBytes(data));
        }

        public void WriteUInt32(uint data) => Write(data);
        public void Write(uint data)
        {
            Stream.Write(BitConverter.GetBytes(data));
        }

        public void WriteInt64(long data) => Write(data);
        public void Write(long data)
        {
            Stream.Write(BitConverter.GetBytes(data));
        }

        public void WriteUInt64(ulong data) => Write(data);
        public void Write(ulong data)
        {
            Stream.Write(BitConverter.GetBytes(data));
        }

        public void Write(char data)
        {
            Stream.WriteByte(Convert.ToByte(data));
        }

        public void Write(string data, bool writeLength = false)
        {
            if (writeLength)
                Write(data.Length);

            var test = Encoding.UTF8.GetBytes(data);
            Write(test);
        }

        public void Write(Enum data)
        {
            Write(Convert.ToByte(data));
        }

        #endregion

        public byte[] GetLeftover()
        {
            return ReadSize(StreamLength - (int)GetPos());
        }

        public byte[] GetBuffer() => Stream.GetBuffer();

        public byte[] GetData()
        {
            Stream.Seek(0, SeekOrigin.Begin);
            return Stream.ToArray();
        }
        
        public long GetPos() => Stream.Position;

        public void SetPos(long pos)
        {
            Stream.Position = pos;
        }
    }
}

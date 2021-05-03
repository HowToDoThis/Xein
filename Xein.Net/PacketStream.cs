using System;
using System.IO;
using System.Text;

namespace Xein.Net
{
    public class PacketStream
    {
        MemoryStream Stream { get; set; }

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
            return Convert.ToByte(Stream.ReadByte());
        }

        public short ReadShort()
        {
            return BitConverter.ToInt16(Read<short>());
        }

        public ushort ReadUShort()
        {
            return BitConverter.ToUInt16(Read<ushort>());
        }

        public int ReadInt()
        {
            return BitConverter.ToInt32(Read<int>());
        }

        public uint ReadUInt()
        {
            return BitConverter.ToUInt32(Read<uint>());
        }

        public long ReadLong()
        {
            return BitConverter.ToInt64(Read<long>());
        }

        public ulong ReadULong()
        {
            return BitConverter.ToUInt64(Read<ulong>());
        }

        public float ReadFloat()
        {
            return BitConverter.ToSingle(Read<float>());
        }

        public double ReadDouble()
        {
            return BitConverter.ToDouble(Read<double>());
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
            //Write((byte)0);
        }

        public void Write(Enum data)
        {
            Write(Convert.ToByte(data));
        }

        #endregion

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

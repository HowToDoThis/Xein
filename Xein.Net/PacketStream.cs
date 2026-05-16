using System;
using System.Buffers.Binary;
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

        /// <summary>
        /// Load a buffer with an explicit logical length (for pooled buffers
        /// where data.Length may exceed the actual packet size).
        /// </summary>
        public PacketStream(byte[] data, int length)
        {
            if ((uint)length > (uint)data.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            // index/count enforces position bounds at [0, length]
            // publiclyVisible: true enables zero-copy GetBuffer() / AsSpan()
            Stream = new MemoryStream(data, 0, length, writable: false, publiclyVisible: true);
            StreamLength = length;
        }

        #region Read
        public string ReadStringA(int size = 0)
        {
            if (size > 0)
            {
                Span<byte> buf = size <= 256 ? stackalloc byte[size] : new byte[size];
                return !TryReadInto(buf) ? string.Empty : Encoding.ASCII.GetString(buf);
            }

            // null-terminated — scan until \0 (cap at a sane max)
            Span<byte> tmp = stackalloc byte[256];
            int len = 0;
            while (len < tmp.Length)
            {
                if (StreamLength - Stream.Position < 1) break;
                int b = Stream.ReadByte();
                if (b <= 0) break;
                tmp[len++] = (byte)b;
            }
            return Encoding.ASCII.GetString(tmp.Slice(0, len));
        }

        public string ReadStringW(int size = 0)
        {
            if (size > 0)
            {
                int bytes = size * 2;
                Span<byte> buf = bytes <= 256 ? stackalloc byte[bytes] : new byte[bytes];
                return !TryReadInto(buf) ? string.Empty : Encoding.Unicode.GetString(buf);
            }

            Span<byte> tmp = stackalloc byte[512];   // 256 wchars
            int len = 0;
            while (len + 1 < tmp.Length)
            {
                if (StreamLength - Stream.Position < 2) break;
                ushort ch = ReadUInt16();
                if (ch == 0) break;
                tmp[len++] = (byte)(ch & 0xFF);
                tmp[len++] = (byte)(ch >> 8);
            }
            return Encoding.Unicode.GetString(tmp.Slice(0, len));
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

        // Helper: read N bytes into a span, return false on short read
        private bool ReadInto(Span<byte> dest)
        {
            return StreamLength - Stream.Position >= dest.Length && Stream.Read(dest) == dest.Length;
        }

        public bool TryReadInto(Span<byte> dest)
        {
            return StreamLength - Stream.Position >= dest.Length && Stream.Read(dest) == dest.Length;
        }

        public byte ReadInt8()
        {
            if (StreamLength - Stream.Position < 1) return 0;
            int b = Stream.ReadByte();
            return b < 0 ? (byte)0 : (byte)b;
        }

        public short ReadInt16(bool isLe = true)
        {
            Span<byte> buf = stackalloc byte[sizeof(short)];
            return !ReadInto(buf)
                ? (short)0
                : isLe ? BinaryPrimitives.ReadInt16LittleEndian(buf)
                        : BinaryPrimitives.ReadInt16BigEndian(buf);
        }

        public ushort ReadUInt16(bool isLe = true)
        {
            Span<byte> buf = stackalloc byte[sizeof(ushort)];
            return !ReadInto(buf)
                ? (ushort)0
                : isLe ? BinaryPrimitives.ReadUInt16LittleEndian(buf)
                        : BinaryPrimitives.ReadUInt16BigEndian(buf);
        }

        public int ReadInt24(bool isLe = true)
        {
            Span<byte> buf = stackalloc byte[4];
            if (StreamLength - Stream.Position < 3) return 0;
            if (isLe)
            {
                if (Stream.Read(buf.Slice(0, 3)) != 3) return 0;
                buf[3] = 0;
            }
            else
            {
                buf[0] = 0;
                if (Stream.Read(buf.Slice(1, 3)) != 3) return 0;
            }
            return BinaryPrimitives.ReadInt32LittleEndian(buf); // already laid out correctly
        }

        public int    ReadInt32 (bool isLe = true) { Span<byte> b = stackalloc byte[4]; return !ReadInto(b) ? 0   : isLe ? BinaryPrimitives.ReadInt32LittleEndian (b) : BinaryPrimitives.ReadInt32BigEndian (b); }
        public uint   ReadUInt32(bool isLe = true) { Span<byte> b = stackalloc byte[4]; return !ReadInto(b) ? 0u  : isLe ? BinaryPrimitives.ReadUInt32LittleEndian(b) : BinaryPrimitives.ReadUInt32BigEndian(b); }
        public long   ReadInt64 (bool isLe = true) { Span<byte> b = stackalloc byte[8]; return !ReadInto(b) ? 0L  : isLe ? BinaryPrimitives.ReadInt64LittleEndian (b) : BinaryPrimitives.ReadInt64BigEndian (b); }
        public ulong  ReadUInt64(bool isLe = true) { Span<byte> b = stackalloc byte[8]; return !ReadInto(b) ? 0ul : isLe ? BinaryPrimitives.ReadUInt64LittleEndian(b) : BinaryPrimitives.ReadUInt64BigEndian(b); }
        public float  ReadFloat (bool isLe = true) { Span<byte> b = stackalloc byte[4]; if (!ReadInto(b)) return 0f; if (!isLe) b.Reverse(); return BitConverter.ToSingle(b); }
        public double ReadDouble(bool isLe = true) { Span<byte> b = stackalloc byte[8]; if (!ReadInto(b)) return 0d; if (!isLe) b.Reverse(); return BitConverter.ToDouble(b); }
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

        public bool IsLeftover() => (StreamLength - (int)GetPos()) > 0;
        public byte[] GetLeftover() => ReadSize(StreamLength - (int)GetPos());

        public long GetPos() => Stream.Position;

        public void SetPos(long pos)
        {
            Stream.Position = pos;
        }

        public byte[] GetData() => Stream.ToArray();
        /// <summary>Live view of full payload — valid until Dispose.</summary>
        public ReadOnlySpan<byte> AsSpan() => Stream.GetBuffer().AsSpan(0, StreamLength);
    }
}

using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Xein.Net
{
    public class PacketStream
    {
        /// <summary>
        /// Internal Stream
        /// </summary>
        public MemoryStream Stream { get; }

        public int StreamLength => (int)Stream.Length;

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
            Stream = new MemoryStream(data, 0, data.Length, writable: false, publiclyVisible: true);
        }

        /// <summary>
        /// Load a buffer with an explicit logical length (for pooled buffers
        /// where data.Length may exceed the actual packet size).
        /// </summary>
        public PacketStream(byte[] data, int length)
        {
            if ((uint)length > (uint)data.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            Stream = new MemoryStream(data, 0, length, writable: false, publiclyVisible: true);
        }

        #region Read
        public byte[] ReadNullStringRaw(bool isWide)
        {
            using var ms = new MemoryStream();
            var size = isWide ? 2 : 1;

            while (StreamLength - Stream.Position >= size)
            {
                int b = isWide ? ReadUInt16() : ReadInt8();
                if (b <= 0) break;

                if (isWide)
                {
                    ms.WriteByte((byte)(b & 0xFF));
                    ms.WriteByte((byte)(b >> 8));
                }
                else
                {
                    ms.WriteByte((byte)b);
                }
            }

            return ms.ToArray();
        }

        private byte[] ReadNullStringSlow(ReadOnlySpan<byte> old, bool isWide)
        {
            using var ms = new MemoryStream();
            ms.Write(old);
            ms.Write(ReadNullStringRaw(isWide));
            return ms.ToArray();
        }

        public string ReadStringA(int size = 0)
        {
            if (size > 0)
            {
                Span<byte> buf = size <= 256 ? stackalloc byte[size] : new byte[size];
                return !TryReadInto(buf) ? string.Empty : Encoding.UTF8.GetString(buf);
            }

            Span<byte> tmp = stackalloc byte[256];
            int len = 0;
            while (len < tmp.Length)
            {
                if (StreamLength - Stream.Position < 1) break;
                int b = Stream.ReadByte();
                if (b <= 0) break;
                tmp[len++] = (byte)b;
            }
            return len == tmp.Length
                    ? Encoding.UTF8.GetString(ReadNullStringSlow(tmp, isWide: false))
                    : Encoding.UTF8.GetString(tmp[..len]);
        }

        public string ReadStringW(int size = 0)
        {
            if (size > 0)
            {
                int bytes = size * 2;
                Span<byte> buf = bytes <= 512 ? stackalloc byte[bytes] : new byte[bytes];
                return !TryReadInto(buf) ? string.Empty : Encoding.Unicode.GetString(buf);
            }

            Span<byte> tmp = stackalloc byte[512];   // 256 wchars
            int len = 0;
            while (len + 2 <= tmp.Length)
            {
                if (StreamLength - Stream.Position < 2) break;
                ushort ch = ReadUInt16();
                if (ch == 0) break;
                tmp[len++] = (byte)(ch & 0xFF);
                tmp[len++] = (byte)(ch >> 8);
            }
            return len == tmp.Length
                    ? Encoding.Unicode.GetString(ReadNullStringSlow(tmp, isWide: true))
                    : Encoding.Unicode.GetString(tmp[..len]);
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
            return !TryReadInto(buf)
                ? (short)0
                : isLe ? BinaryPrimitives.ReadInt16LittleEndian(buf)
                        : BinaryPrimitives.ReadInt16BigEndian(buf);
        }

        public ushort ReadUInt16(bool isLe = true)
        {
            Span<byte> buf = stackalloc byte[sizeof(ushort)];
            return !TryReadInto(buf)
                ? (ushort)0
                : isLe ? BinaryPrimitives.ReadUInt16LittleEndian(buf)
                        : BinaryPrimitives.ReadUInt16BigEndian(buf);
        }

        public int ReadInt24(bool isLe = true)
        {
            if (StreamLength - Stream.Position < 3) return 0;
            Span<byte> buf = stackalloc byte[4];

            if (isLe)
            {
                if (Stream.Read(buf[..3]) != 3) return 0;
                buf[3] = 0;
                return BinaryPrimitives.ReadInt32LittleEndian(buf);
            }
            else
            {
                buf[0] = 0;
                if (Stream.Read(buf[1..]) != 3) return 0;
                return BinaryPrimitives.ReadInt32BigEndian(buf);
            }
        }
        public int    ReadInt32 (bool isLe = true) { Span<byte> b = stackalloc byte[4]; return !TryReadInto(b) ? 0   : isLe ? BinaryPrimitives.ReadInt32LittleEndian (b) : BinaryPrimitives.ReadInt32BigEndian (b); }
        public uint   ReadUInt32(bool isLe = true) { Span<byte> b = stackalloc byte[4]; return !TryReadInto(b) ? 0u  : isLe ? BinaryPrimitives.ReadUInt32LittleEndian(b) : BinaryPrimitives.ReadUInt32BigEndian(b); }
        public long   ReadInt64 (bool isLe = true) { Span<byte> b = stackalloc byte[8]; return !TryReadInto(b) ? 0L  : isLe ? BinaryPrimitives.ReadInt64LittleEndian (b) : BinaryPrimitives.ReadInt64BigEndian (b); }
        public ulong  ReadUInt64(bool isLe = true) { Span<byte> b = stackalloc byte[8]; return !TryReadInto(b) ? 0ul : isLe ? BinaryPrimitives.ReadUInt64LittleEndian(b) : BinaryPrimitives.ReadUInt64BigEndian(b); }
        public float  ReadFloat (bool isLe = true) { Span<byte> b = stackalloc byte[4]; return !TryReadInto(b) ? 0f  : isLe ? BinaryPrimitives.ReadSingleLittleEndian(b) : BinaryPrimitives.ReadSingleBigEndian(b); }
        public double ReadDouble(bool isLe = true) { Span<byte> b = stackalloc byte[8]; return !TryReadInto(b) ? 0d  : isLe ? BinaryPrimitives.ReadDoubleLittleEndian(b) : BinaryPrimitives.ReadDoubleBigEndian(b); }
        #endregion

        #region Write
        public void Write(byte[] data) => Stream.Write(data);
        public void Write(ReadOnlySpan<byte> data) => Stream.Write(data);
        public void Write(byte data) => Stream.WriteByte(data);
        public void Write(bool data) => Stream.WriteByte(data ? (byte)1 : (byte)0);
        public void Write(Enum data) => Stream.WriteByte(Convert.ToByte(data));

        public void WriteInt8(int data) => Stream.WriteByte((byte)data);

        public void WriteInt16(int data, bool isLe = true)
        {
            Span<byte> b = stackalloc byte[2];
            if (isLe) BinaryPrimitives.WriteInt16LittleEndian(b, (short)data);
            else BinaryPrimitives.WriteInt16BigEndian(b, (short)data);
            Stream.Write(b);
        }

        public void WriteUInt16(int data, bool isLe = true)
        {
            Span<byte> b = stackalloc byte[2];
            if (isLe) BinaryPrimitives.WriteUInt16LittleEndian(b, (ushort)data);
            else BinaryPrimitives.WriteUInt16BigEndian(b, (ushort)data);
            Stream.Write(b);
        }

        public void WriteInt24(int data, bool isLe = true)
        {
            Span<byte> b = stackalloc byte[4];
            if (isLe)
            {
                BinaryPrimitives.WriteInt32LittleEndian(b, data);
                Stream.Write(b[..3]); // low 3
            }
            else
            {
                BinaryPrimitives.WriteInt32BigEndian(b, data);
                Stream.Write(b[1..]); // high 3
            }
        }

        public void WriteInt32 (int   data, bool isLe = true) { Span<byte> b = stackalloc byte[4]; if (isLe) BinaryPrimitives.WriteInt32LittleEndian (b, data); else BinaryPrimitives.WriteInt32BigEndian (b, data); Stream.Write(b); }
        public void WriteUInt32(uint  data, bool isLe = true) { Span<byte> b = stackalloc byte[4]; if (isLe) BinaryPrimitives.WriteUInt32LittleEndian(b, data); else BinaryPrimitives.WriteUInt32BigEndian(b, data); Stream.Write(b); }
        public void WriteInt64 (long  data, bool isLe = true) { Span<byte> b = stackalloc byte[8]; if (isLe) BinaryPrimitives.WriteInt64LittleEndian (b, data); else BinaryPrimitives.WriteInt64BigEndian (b, data); Stream.Write(b); }
        public void WriteUInt64(ulong data, bool isLe = true) { Span<byte> b = stackalloc byte[8]; if (isLe) BinaryPrimitives.WriteUInt64LittleEndian(b, data); else BinaryPrimitives.WriteUInt64BigEndian(b, data); Stream.Write(b); }

        public void WriteFloat(float data, bool isLe = true)
        {
            Span<byte> b = stackalloc byte[4];
            if (isLe) BinaryPrimitives.WriteSingleLittleEndian(b, data);
            else BinaryPrimitives.WriteSingleBigEndian(b, data);
            Stream.Write(b);
        }

        public void WriteDouble(double data, bool isLe = true)
        {
            Span<byte> b = stackalloc byte[8];
            if (isLe) BinaryPrimitives.WriteDoubleLittleEndian(b, data);
            else BinaryPrimitives.WriteDoubleBigEndian(b, data);
            Stream.Write(b);
        }

        public void WriteStringA(string data)
        {
            int max = Encoding.UTF8.GetMaxByteCount(data.Length);
            Span<byte> buf = max <= 256 ? stackalloc byte[max] : new byte[max];
            int n = Encoding.UTF8.GetBytes(data, buf);
            Stream.Write(buf[..n]);
        }

        public void WriteStringW(string data)
        {
            int bytes = data.Length * 2;
            Span<byte> buf = bytes <= 512 ? stackalloc byte[bytes] : new byte[bytes];
            int n = Encoding.Unicode.GetBytes(data, buf);
            Stream.Write(buf[..n]);
        }
        #endregion

        public bool   IsLeftover()  => (StreamLength - (int)GetPos()) > 0;
        public byte[] GetLeftover() => ReadSize(StreamLength - (int)GetPos());

        public long GetPos() => Stream.Position;

        public void SetPos(long pos)
        {
            Stream.Position = pos;
        }

        public byte[] GetData() => Stream.ToArray();
        public ReadOnlySpan<byte> AsSpan() => Stream.GetBuffer().AsSpan(0, (int)Stream.Length);
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Fugu.Common
{
    /// <summary>
    /// A continguous range of bytes backed by unmanaged memory.
    /// </summary>
    public struct ManagedByteSpan : IByteSpan<ManagedByteSpan>
    {
        private readonly byte[] _buffer;
        private readonly int _offset;
        private readonly int _length;

        public ManagedByteSpan(byte[] memory)
            : this(memory, 0, memory.Length)
        {
        }

        public ManagedByteSpan(byte[] memory, int offset, int length)
        {
            Guard.NotNull(memory, nameof(memory));

            if (offset < 0 || offset > memory.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            _buffer = memory;
            _offset = offset;
            _length = length;
        }

        public long Length => _length;

        public byte this[long index]
        {
            get
            {
                if (index < 0 || index >= _length)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }

                return _buffer[_offset + index];
            }

            set
            {
                if (index < 0 || index >= _length)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }

                _buffer[_offset + index] = value;
            }
        }

        public ManagedByteSpan Slice(long startIndex)
        {
            return Slice(startIndex, _length - startIndex);
        }

        public unsafe ManagedByteSpan Slice(long startIndex, long length)
        {
            if (startIndex < 0 || startIndex > _length)
            {
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            }

            if (length < 0 || startIndex + length > _length)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            return new ManagedByteSpan(_buffer, _offset + (int)startIndex, (int)length);
        }

        public unsafe T Read<T>() where T : struct
        {
            if (Unsafe.SizeOf<T>() > _length)
            {
                throw new InvalidOperationException();
            }

            fixed (byte* ptr = _buffer)
            {
                return Unsafe.Read<T>(ptr + _offset);
            }
        }

        public unsafe void CopyTo(byte[] destinationArray)
        {
            Array.Copy(_buffer, _offset, destinationArray, 0, _length);
        }

        public unsafe ManagedByteSpan Write<T>(ref T item) where T : struct
        {
            var size = Unsafe.SizeOf<T>();
            if (size > _length)
            {
                throw new ArgumentException(nameof(item));
            }

            fixed (byte* ptr = _buffer)
            {
                Unsafe.Copy(ptr + _offset, ref item);
            }

            return Slice(size);
        }

        public ManagedByteSpan Write(byte[] sourceArray, int sourceIndex, int length)
        {
            Guard.NotNull(sourceArray, nameof(sourceArray));

            if (sourceIndex < 0 || sourceIndex > _length)
            {
                throw new ArgumentOutOfRangeException(nameof(sourceIndex));
            }

            if (length < 0 || sourceIndex + length > _length)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            Array.Copy(sourceArray, sourceIndex, _buffer, _offset, length);

            return Slice(length);
        }
    }
}

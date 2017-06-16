using System;
using System.Runtime.CompilerServices;

namespace Fugu.Common
{
    /// <summary>
    /// A continguous range of bytes backed by unmanaged memory.
    /// </summary>
    public struct UnmanagedByteSpan : IByteSpan<UnmanagedByteSpan>
    {
        // Backing memory
        private unsafe readonly byte* _memory;

        // Size, in bytes, of backing memory
        private readonly long _length;

        public unsafe UnmanagedByteSpan(byte* memory, long length)
        {
            if (memory == null)
            {
                throw new ArgumentNullException(nameof(memory));
            }

            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            _memory = memory;
            _length = length;
        }

        public long Length => _length;

        public unsafe byte this[long index]
        {
            get
            {
                if (index < 0 || index >= _length)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }

                return *(_memory + index);
            }

            set
            {
                if (index < 0 || index >= _length)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }

                *(_memory + index) = value;
            }
        }

        public UnmanagedByteSpan Slice(long startIndex)
        {
            return Slice(startIndex, _length - startIndex);
        }

        public unsafe UnmanagedByteSpan Slice(long startIndex, long length)
        {
            if (startIndex < 0 || startIndex > _length)
            {
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            }

            if (length < 0 || startIndex + length > _length)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            return new UnmanagedByteSpan(_memory + startIndex, length);
        }

        public unsafe T Read<T>() where T : struct
        {
            if (Unsafe.SizeOf<T>() > _length)
            {
                throw new InvalidOperationException();
            }

            return Unsafe.Read<T>(_memory);
        }

        public unsafe void CopyTo(byte[] destinationArray)
        {
            fixed (byte* destination = destinationArray)
            {
                Unsafe.CopyBlock(destination, _memory, (uint)_length);
            }
        }

        public unsafe UnmanagedByteSpan Write<T>(ref T item) where T : struct
        {
            var size = Unsafe.SizeOf<T>();
            if (size > _length)
            {
                throw new ArgumentException(nameof(item));
            }

            Unsafe.Copy(_memory, ref item);
            return Slice(size);
        }

        public unsafe UnmanagedByteSpan Write(byte[] sourceArray, int sourceIndex, int length)
        {
            if (sourceArray == null)
            {
                throw new ArgumentNullException(nameof(sourceArray));
            }

            if (sourceIndex < 0 || sourceIndex > _length)
            {
                throw new ArgumentOutOfRangeException(nameof(sourceIndex));
            }

            if (length < 0 || sourceIndex + length > _length)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            fixed (byte* source = sourceArray)
            {
                Unsafe.CopyBlock(_memory, source + sourceIndex, (uint)length);
            }

            return Slice(length);
        }
    }
}

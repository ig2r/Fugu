using System;
using System.IO;

namespace Fugu.TableSets
{
    public class InMemoryTable : IWritableTable
    {
        private readonly byte[] _buffer;

        public InMemoryTable(long capacity)
        {
            if (capacity <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity));
            }

            Capacity = capacity;
            _buffer = new byte[Capacity];
        }

        public long Capacity { get; }

        public Stream GetInputStream(long position, long size)
        {
            if (position > Capacity)
            {
                throw new ArgumentOutOfRangeException(nameof(position));
            }

            if (position + size > Capacity)
            {
                throw new ArgumentOutOfRangeException(nameof(size));
            }

            return new MemoryStream(_buffer, (int)position, (int)size, writable: false);
        }

        public Stream GetOutputStream(long position, long size)
        {
            if (position > Capacity)
            {
                throw new ArgumentOutOfRangeException(nameof(position));
            }

            if (position + size > Capacity)
            {
                throw new ArgumentOutOfRangeException(nameof(size));
            }

            return new MemoryStream(_buffer, (int)position, (int)size, writable: true);
        }
    }
}

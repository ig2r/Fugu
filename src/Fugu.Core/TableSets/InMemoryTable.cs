using System;
using System.IO;
using Fugu.Common;
using Fugu.Format;

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

            if (capacity > int.MaxValue)
            {
                throw new InvalidOperationException($"InMemoryTable supports a maximum capacity of ${int.MaxValue} bytes.");
            }

            Capacity = capacity;
            _buffer = new byte[Capacity];
        }

        public long Capacity { get; }

        public TableWriter GetWriter()
        {
            var span = new ManagedByteSpan(_buffer);
            return new ByteSpanTableWriter<ManagedByteSpan>(span);
        }

        public TableReader GetReader(long position, long size)
        {
            if (position > Capacity)
            {
                throw new ArgumentOutOfRangeException(nameof(position));
            }

            if (position + size > Capacity)
            {
                throw new ArgumentOutOfRangeException(nameof(size));
            }

            var span = new ManagedByteSpan(_buffer, (int)position, (int)size);
            return new ByteSpanTableReader<ManagedByteSpan>(span);
        }
    }
}

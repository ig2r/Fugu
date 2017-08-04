using System;

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

        public Span<byte> GetSpan(long offset)
        {
            if (offset < 0 || offset > Capacity)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            checked
            {
                int length = (int)(Capacity - offset);
                return new Span<byte>(_buffer, (int)offset, length);
            }
        }
    }
}

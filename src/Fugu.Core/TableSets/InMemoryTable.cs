using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.TableSets
{
    public class InMemoryTable : IWritableTable, ITable
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
            OutputStream = new MemoryStream(_buffer);
        }

        public long Capacity { get; }
        public Stream OutputStream { get; }

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

            return new MemoryStream(_buffer, (int)position, (int)size);
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.TableSets
{
    public class MemoryMappedTable : IWritableTable, IDisposable
    {
        private readonly MemoryMappedFile _map;

        public MemoryMappedTable(long capacity)
        {
            Capacity = capacity;
            _map = MemoryMappedFile.CreateNew(null, capacity);
            OutputStream = _map.CreateViewStream();
        }

        public long Capacity { get; }
        public Stream OutputStream { get; }

        public Stream GetInputStream(long position, long size)
        {
            return _map.CreateViewStream(position, size, MemoryMappedFileAccess.Read);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                OutputStream?.Dispose();
                _map.Dispose();
            }
        }
    }
}

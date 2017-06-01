using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Fugu.TableSets
{
    public class MemoryMappedTable : ITable, IDisposable
    {
        private readonly MemoryMappedFile _map;

        public MemoryMappedTable(long capacity)
        {
            Capacity = capacity;
            _map = MemoryMappedFile.CreateNew(null, capacity);
        }

        public long Capacity { get; }

        public Stream GetInputStream(long position, long size)
        {
            return _map.CreateViewStream(position, size, MemoryMappedFileAccess.Read);
        }

        public Stream GetOutputStream(long position, long size)
        {
            return _map.CreateViewStream(position, size, MemoryMappedFileAccess.Write);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                _map.Dispose();
            }
        }
    }
}

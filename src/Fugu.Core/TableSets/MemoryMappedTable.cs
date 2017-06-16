using Fugu.Common;
using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Fugu.TableSets
{
    public class MemoryMappedTable : IWritableTable, IDisposable
    {
        private readonly MemoryMappedFile _map;

        public MemoryMappedTable(string path)
        {
            Guard.NotNull(path, nameof(path));

            Path = path;
            Capacity = new FileInfo(path).Length;
            _map = MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        }

        public MemoryMappedTable(string path, long capacity)
        {
            Guard.NotNull(path, nameof(path));

            Path = path;
            Capacity = capacity;
            _map = MemoryMappedFile.CreateFromFile(path, FileMode.OpenOrCreate, null, capacity, MemoryMappedFileAccess.ReadWrite);
        }

        public string Path { get; }
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

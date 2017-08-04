using Fugu.Common;
using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Fugu.TableSets
{
    public class MemoryMappedTable : IWritableTable, IDisposable
    {
        private readonly MemoryMappedFile _map;
        private readonly MemoryMappedViewAccessor _viewAccessor;
        private readonly unsafe byte* _memory;

        private bool _disposed = false;

        public MemoryMappedTable(string path)
        {
            Guard.NotNull(path, nameof(path));

            Path = path;
            Capacity = new FileInfo(path).Length;
            _map = MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
            _viewAccessor = _map.CreateViewAccessor(0, Capacity, MemoryMappedFileAccess.Read);

            unsafe
            {
                _viewAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _memory);
            }
        }

        public MemoryMappedTable(string path, long capacity)
        {
            Guard.NotNull(path, nameof(path));

            Path = path;
            Capacity = capacity;
            _map = MemoryMappedFile.CreateFromFile(path, FileMode.OpenOrCreate, null, capacity, MemoryMappedFileAccess.ReadWrite);
            _viewAccessor = _map.CreateViewAccessor(0, Capacity, MemoryMappedFileAccess.ReadWrite);

            unsafe
            {
                _viewAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _memory);
            }
        }

        public string Path { get; }
        public long Capacity { get; }

        public Span<byte> GetSpan(long offset)
        {
            if (offset < 0 || offset > Capacity)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            var length = (int)Math.Min(Capacity - offset, int.MaxValue);

            unsafe
            {
                return new Span<byte>(_memory + offset, length);
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                _disposed = true;

                _viewAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
                _viewAccessor.Dispose();
                _map.Dispose();
            }
        }
    }
}

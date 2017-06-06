using Fugu.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.TableSets
{
    public class MemoryMappedTableSet : ITableSet, IDisposable
    {
        private readonly string _basePath;
        private readonly HashSet<MemoryMappedTable> _tables = new HashSet<MemoryMappedTable>();

        public MemoryMappedTableSet(string basePath)
        {
            Guard.NotNull(basePath, nameof(basePath));
            _basePath = basePath;

            var files = Directory.GetFiles(_basePath);
            foreach (var file in files)
            {
                _tables.Add(new MemoryMappedTable(file));
            }
        }

        public Task<IWritableTable> CreateTableAsync(long capacity)
        {
            var fileName = Guid.NewGuid().ToString() + ".log";
            var path = Path.Combine(_basePath, fileName);

            var table = new MemoryMappedTable(path, capacity);
            lock (_tables)
            {
                _tables.Add(table);
            }

            return Task.FromResult<IWritableTable>(table);
        }

        public Task<IEnumerable<ITable>> GetTablesAsync()
        {
            lock (_tables)
            {
                return Task.FromResult<IEnumerable<ITable>>(_tables.ToArray());
            }
        }

        public Task RemoveTableAsync(ITable table)
        {
            if (table is MemoryMappedTable memoryMappedTable)
            {
                bool proceedWithRemoval;
                lock (_tables)
                {
                    proceedWithRemoval = _tables.Remove(memoryMappedTable);
                }

                if (proceedWithRemoval)
                {
                    memoryMappedTable.Dispose();
                    File.Delete(memoryMappedTable.Path);
                }
            }

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (_tables)
                {
                    foreach (var table in _tables)
                    {
                        table.Dispose();
                    }

                    _tables.Clear();
                }
            }
        }
    }
}

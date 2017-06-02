using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.TableSets
{
    public class MemoryMappedTableSet : ITableSet, IDisposable
    {
        private readonly HashSet<MemoryMappedTable> _tables = new HashSet<MemoryMappedTable>();

        public Task<ITable> CreateTableAsync(long capacity)
        {
            var table = new MemoryMappedTable(capacity);
            lock (_tables)
            {
                _tables.Add(table);
            }

            return Task.FromResult<ITable>(table);
        }

        public Task<IEnumerable<ITable>> GetTablesAsync()
        {
            lock (_tables)
            {
                return Task.FromResult<IEnumerable<ITable>>(_tables.ToArray());
            }
        }

        public Task RemoveTableAsync(IReadOnlyTable table)
        {
            var memoryMappedTable = (MemoryMappedTable)table;
            lock (_tables)
            {
                _tables.Remove(memoryMappedTable);
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.TableSets
{
    public class InMemoryTableSet : ITableSet
    {
        private readonly HashSet<InMemoryTable> _tables = new HashSet<InMemoryTable>();

        #region ITableSet

        public Task<ITable> CreateTableAsync(long capacity)
        {
            var table = new InMemoryTable(capacity);

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

        public Task RemoveTableAsync(ITable table)
        {
            var memoryTable = (InMemoryTable)table;

            lock (_tables)
            {
                if (!_tables.Remove(memoryTable))
                {
                    throw new InvalidOperationException("Unknown table.");
                }
            }

            return Task.CompletedTask;
        }

        #endregion
    }
}

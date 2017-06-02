using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu
{
    public interface ITableSet : ITableFactory
    {
        Task<IEnumerable<ITable>> GetTablesAsync();
        Task RemoveTableAsync(IReadOnlyTable table);
    }
}

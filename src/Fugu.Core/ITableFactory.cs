using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu
{
    public interface ITableFactory
    {
        Task<ITable> CreateTableAsync(long capacity);
    }
}

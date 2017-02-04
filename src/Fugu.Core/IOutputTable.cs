using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu
{
    public interface IOutputTable : ITable
    {
        Stream OutputStream { get; }
    }
}

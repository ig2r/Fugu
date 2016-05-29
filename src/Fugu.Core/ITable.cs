using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu
{
    public interface ITable
    {
        long Capacity { get; }
        Stream GetInputStream(long position, long size);
    }
}

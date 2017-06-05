using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Fugu.Bootstrapping
{
    public class BootstrapperResult
    {
        public BootstrapperResult(long maxGeneration, long totalCapacity)
        {
            if (maxGeneration < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxGeneration));
            }

            if (totalCapacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(totalCapacity));
            }

            MaxGeneration = maxGeneration;
            TotalCapacity = totalCapacity;
        }

        public long MaxGeneration { get; }
        public long TotalCapacity { get; }
    }
}

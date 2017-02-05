using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public class BootstrapperResult
    {
        public BootstrapperResult(long maxGenerationLoaded)
        {
            MaxGenerationLoaded = maxGenerationLoaded;
        }

        public long MaxGenerationLoaded { get; }
    }
}

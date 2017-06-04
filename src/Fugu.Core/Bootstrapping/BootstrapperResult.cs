using Fugu.Common;
using System.Collections.Generic;
using System.Linq;

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

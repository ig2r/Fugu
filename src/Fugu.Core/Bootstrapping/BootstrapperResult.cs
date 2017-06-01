using Fugu.Common;
using System.Collections.Generic;
using System.Linq;

namespace Fugu.Bootstrapping
{
    public class BootstrapperResult
    {
        public BootstrapperResult(long maxGenerationLoaded, IEnumerable<Segment> loadedSegments)
        {
            Guard.NotNull(loadedSegments, nameof(loadedSegments));

            MaxGenerationLoaded = maxGenerationLoaded;
            LoadedSegments = loadedSegments.ToArray();
        }

        public long MaxGenerationLoaded { get; }
        public IEnumerable<Segment> LoadedSegments { get; }
    }
}

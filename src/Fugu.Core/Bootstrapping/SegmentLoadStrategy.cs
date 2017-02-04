using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    /// <summary>
    /// Given a list of segments available in a table set, this strategy determines an ordering to load data
    /// back into the in-memory index.
    /// </summary>
    public class SegmentLoadStrategy
    {
        public async Task<IReadOnlyList<Segment>> RunAsync(IEnumerable<Segment> segments, ISegmentLoader tableLoader)
        {
            var ordered = (from s in segments
                           orderby
                             s.MinGeneration ascending,
                             s.MaxGeneration descending
                           select s).ToArray();

            long maxGenerationLoaded = 0;
            var loadedSegments = new List<Segment>();

            for (int i = 0; i < ordered.Length; i++)
            {
                var current = ordered[i];
                if (current.MinGeneration > maxGenerationLoaded)
                {
                    // This segment contains data from a generation we haven't touched yet, check if has a valid footer
                    var hasFooter = await tableLoader.CheckTableFooterAsync(current.Table);

                    if (!hasFooter)
                    {
                        var nextSegmentCoversSameGeneration =
                            i < ordered.Length - 1 && ordered[i + 1].MinGeneration == current.MinGeneration;

                        if (nextSegmentCoversSameGeneration)
                        {
                            // Skip this segment entirely
                            continue;
                        }
                    }

                    // Load data from this segment, verifying checksums only if the segment has no valid footer
                    await tableLoader.LoadSegmentAsync(current, verifyChecksums: !hasFooter);

                    maxGenerationLoaded = current.MaxGeneration;
                    loadedSegments.Add(current);
                }
            }

            return loadedSegments;
        }
    }
}

using Fugu.Common;
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
        public async Task RunAsync(IEnumerable<Segment> segments, ISegmentLoader segmentLoader)
        {
            Guard.NotNull(segments, nameof(segments));
            Guard.NotNull(segmentLoader, nameof(segmentLoader));

            // Process segments in order, giving preference to segments that span larger generation
            // ranges since they are most likely more recent (e.g., created during compactions) than
            // segments that span smaller ranges
            var queue = new Queue<Segment>(
                from s in segments
                orderby
                    s.MinGeneration ascending,
                    s.MaxGeneration descending
                select s);
            long maxGenerationLoaded = 0;

            while (queue.Count > 0)
            {
                var current = queue.Dequeue();

                // Skip this segment if it contains data from a generation range we've already loaded
                if (current.MaxGeneration <= maxGenerationLoaded)
                {
                    continue;
                }

                // If the following segment covers the same min generation range as the current segment,
                // we can be picky and only accept data from the current segment if it contains a valid
                // footer; if it doesn't, we'll just fall back to the next segment in line
                bool requireValidFooter = false;
                if (queue.Count > 0)
                {
                    var next = queue.Peek();
                    if (current.MinGeneration == next.MinGeneration)
                    {
                        requireValidFooter = true;
                    }
                }

                // Now load it
                if (await segmentLoader.TryLoadSegmentAsync(current, requireValidFooter))
                {
                    maxGenerationLoaded = current.MaxGeneration;
                }
            }
        }
    }
}

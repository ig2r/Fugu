using Fugu.Common;
using System.Collections.Generic;
using System.Linq;

namespace Fugu.Bootstrapping
{
    /// <summary>
    /// Given a list of segments available in a table set, this strategy determines an ordering to load data
    /// back into the in-memory index.
    /// </summary>
    public class SegmentLoadStrategy
    {
        private readonly Queue<Segment> _queue;

        public SegmentLoadStrategy(IEnumerable<Segment> segments)
        {
            Guard.NotNull(segments, nameof(segments));

            // Process segments in order, giving preference to segments that span larger generation
            // ranges since they are most likely more recent (e.g., created during compactions) than
            // segments that span smaller ranges
            _queue = new Queue<Segment>(
                from s in segments
                orderby
                    s.MinGeneration ascending,
                    s.MaxGeneration descending
                select s);
        }

        public bool GetNext(long maxGenerationLoaded, out Segment nextSegment, out bool requireValidFooter)
        {
            while (_queue.Count > 0)
            {
                nextSegment = _queue.Dequeue();

                // Skip this segment if it contains data from a generation range we've already loaded
                if (nextSegment.MaxGeneration <= maxGenerationLoaded)
                {
                    continue;
                }

                // If the following segment covers the same min generation range as the current segment,
                // we can be picky and only accept data from the current segment if it contains a valid
                // footer; if it doesn't, we'll just fall back to the next segment in line
                requireValidFooter =
                    _queue.Count > 0 &&
                    nextSegment.MinGeneration == _queue.Peek().MinGeneration;
                return true;
            }

            nextSegment = null;
            requireValidFooter = false;
            return false;
        }
    }
}

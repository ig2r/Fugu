using Fugu.Channels;
using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    /// <summary>
    /// Monitors data distribution across segments in the store, and evicts/merges segments to retain balance.
    /// </summary>
    public class CompactionActorCore
    {
        private readonly Channel<EvictSegmentMessage> _evictSegmentChannel;
        private readonly Dictionary<Segment, SegmentStats> _segmentStats = new Dictionary<Segment, SegmentStats>();

        public CompactionActorCore(Channel<EvictSegmentMessage> evictSegmentChannel)
        {
            Guard.NotNull(evictSegmentChannel, nameof(evictSegmentChannel));
            _evictSegmentChannel = evictSegmentChannel;
        }

        public async Task OnSegmentSizesChangedAsync(
            StateVector clock,
            IReadOnlyList<KeyValuePair<Segment, SegmentSizeChange>> sizeChanges,
            CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> index)
        {
            // Apply updates
            foreach (var (key, change) in sizeChanges)
            {
                var stats = _segmentStats.TryGetValue(key, out var existingStats)
                    ? existingStats
                    : default(SegmentStats);

                _segmentStats[key] = stats + change;
            }

            // Look for segments that no longer hold any useful data and schedule them for eviction
            var emptySegments = _segmentStats
                .Where(kvp => kvp.Key.MaxGeneration < clock.OutputGeneration && kvp.Value.LiveBytes == 0)
                .Select(kvp => kvp.Key)
                .ToArray();

            foreach (var s in emptySegments)
            {
                _segmentStats.Remove(s);
                await _evictSegmentChannel.SendAsync(new EvictSegmentMessage(clock, s));
            }

            // Check if we need to compact a range of segments
            var compactableStats = _segmentStats
                .Where(kvp => kvp.Key.MaxGeneration < clock.OutputGeneration)
                .OrderBy(kvp => kvp.Key.MinGeneration)
                .ToArray();
        }
    }
}

using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class EvictionActor : IEvictionActor
    {
        private readonly MessageLoop _messageLoop = new MessageLoop();
        private readonly ITableSet _tableSet;

        private readonly HashSet<KeyValuePair<VectorClock, Segment>> _scheduledForEviction =
            new HashSet<KeyValuePair<VectorClock, Segment>>();
        private VectorClock _oldestActiveClock = new VectorClock();

        public EvictionActor(ITableSet tableSet)
        {
            Guard.NotNull(tableSet, nameof(tableSet));
            _tableSet = tableSet;
        }

        /// <summary>
        /// Accepts a segment that should be removed from the hive when the store's logical clock
        /// has reached (or surpassed) a given point in time.
        /// </summary>
        /// <param name="evictAt">The logical clock value at which the segment may be purged.</param>
        /// <param name="segment">The segment to purge.</param>
        public async void ScheduleEviction(VectorClock evictAt, Segment segment)
        {
            Guard.NotNull(segment, nameof(segment));

            using (await _messageLoop)
            {
                _scheduledForEviction.Add(new KeyValuePair<VectorClock, Segment>(evictAt, segment));
                await EvictUnusedSegmentsAsync();
            }
        }

        /// <summary>
        /// Notifies the current component that the oldest clock that is still visible for snapshots
        /// has advanced.
        /// </summary>
        /// <param name="oldestActiveClock">The newly-updated oldest logical clock value that is still
        /// visible in snapshots.</param>
        public async void OnOldestLiveSnapshotChanged(VectorClock oldestActiveClock)
        {
            using (await _messageLoop)
            {
                _oldestActiveClock = VectorClock.Merge(_oldestActiveClock, oldestActiveClock);
                await EvictUnusedSegmentsAsync();
            }
        }

        private async Task EvictUnusedSegmentsAsync()
        {
            var evictable = _scheduledForEviction.Where(kvp => kvp.Key <= _oldestActiveClock).ToArray();

            foreach (var kvp in evictable)
            {
                await _tableSet.RemoveTableAsync(kvp.Value.Table);
                _scheduledForEviction.Remove(kvp);
            }
        }
    }
}

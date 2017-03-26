using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Fugu.Common;

namespace Fugu.Actors
{
    public class EvictionActorCore
    {
        private readonly ITableSet _tableSet;
        private readonly Queue<(StateVector evictAt, Segment segment)> _queued = new Queue<(StateVector evictAt, Segment segment)>();
        private StateVector _oldestVisibleState = default(StateVector);

        public EvictionActorCore(ITableSet tableSet)
        {
            Guard.NotNull(tableSet, nameof(tableSet));
            _tableSet = tableSet;
        }

        public Task EvictSegmentAsync(StateVector evictAt, Segment segment)
        {
            // Assumes that evictAt increases monontonically from call to call, otherwise we couldn't use a queue
            _queued.Enqueue((evictAt, segment));
            return ProcessQueuedEvictionsAsync();
        }

        public Task OnOldestVisibleStateChangedAsync(StateVector clock)
        {
            _oldestVisibleState = clock;
            return ProcessQueuedEvictionsAsync();
        }

        private async Task ProcessQueuedEvictionsAsync()
        {
            while (_queued.Count > 0 && _oldestVisibleState >= _queued.Peek().evictAt)
            {
                var (_, segment) = _queued.Dequeue();
                await _tableSet.RemoveTableAsync(segment.Table);
            }
        }
    }
}

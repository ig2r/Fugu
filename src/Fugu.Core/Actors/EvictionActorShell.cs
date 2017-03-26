using Fugu.Channels;
using Fugu.Common;

namespace Fugu.Actors
{
    public class EvictionActorShell
    {
        private readonly EvictionActorCore _core;
        private readonly Channel<EvictSegmentMessage> _evictSegmentChannel;
        private readonly Channel<StateVector> _oldestVisibleStateChangedChannel;

        public EvictionActorShell(
            EvictionActorCore core,
            Channel<EvictSegmentMessage> evictSegmentChannel,
            Channel<StateVector> oldestVisibleStateChangedChannel)
        {
            Guard.NotNull(core, nameof(core));
            Guard.NotNull(evictSegmentChannel, nameof(evictSegmentChannel));
            Guard.NotNull(oldestVisibleStateChangedChannel, nameof(oldestVisibleStateChangedChannel));

            _core = core;
            _evictSegmentChannel = evictSegmentChannel;
            _oldestVisibleStateChangedChannel = oldestVisibleStateChangedChannel;
        }

        public async void Run()
        {
            await new SelectBuilder()
                .Case(_evictSegmentChannel, msg => _core.EvictSegmentAsync(msg.EvictAt, msg.Segment))
                .Case(_oldestVisibleStateChangedChannel, clock => _core.OnOldestVisibleStateChangedAsync(clock))
                .SelectAsync(_ => true);
        }
    }
}

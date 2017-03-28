using Fugu.Actors;
using Fugu.Common;

namespace Fugu.Compaction
{
    public class CompactionActorShell : ICompactionActor
    {
        private readonly CompactionActorCore _core;
        private readonly Channel<SegmentSizesChangedMessage> _indexUpdatedChannel;

        public CompactionActorShell(
            CompactionActorCore core,
            Channel<SegmentSizesChangedMessage> indexUpdatedChannel)
        {
            Guard.NotNull(core, nameof(core));
            Guard.NotNull(indexUpdatedChannel, nameof(indexUpdatedChannel));

            _core = core;
            _indexUpdatedChannel = indexUpdatedChannel;
        }

        public async void Run()
        {
            await new SelectBuilder()
                .Case(_indexUpdatedChannel, msg => _core.OnSegmentSizesChangedAsync(msg.Clock, msg.SizeChanges, msg.Index))
                .SelectAsync(_ => true);
        }
    }
}

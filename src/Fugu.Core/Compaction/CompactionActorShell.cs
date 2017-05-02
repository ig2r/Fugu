using Fugu.Actors;
using Fugu.Common;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Compaction
{
    public class CompactionActorShell : ICompactionActor
    {
        public CompactionActorShell(CompactionActorCore core)
        {
            Guard.NotNull(core, nameof(core));

            // Unbounded input -- needs to be throttled within sender, i.e., index actor
            SegmentSizesChangedBlock = new ActionBlock<SegmentSizesChangedMessage>(msg =>
                core.OnSegmentSizesChangedAsync(msg.Clock, msg.SizeChanges, msg.Index));
        }

        public ITargetBlock<SegmentSizesChangedMessage> SegmentSizesChangedBlock { get; }
    }
}

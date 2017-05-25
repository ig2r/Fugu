using Fugu.Actors;
using Fugu.Common;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Snapshots
{
    public class SnapshotsActorShell
    {
        private readonly ActionBlock<Snapshot> _snapshotDisposedBlock;

        public SnapshotsActorShell(SnapshotsActorCore core)
        {
            Guard.NotNull(core, nameof(core));

            var scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
            SnapshotsUpdateBlock = new ActionBlock<SnapshotsUpdateMessage>(
                msg => core.UpdateIndexAsync(msg.Clock, msg.Index, msg.ReplyChannel),
                new ExecutionDataflowBlockOptions { TaskScheduler = scheduler, BoundedCapacity = KeyValueStore.DEFAULT_BOUNDED_CAPACITY });

            // Unconstrained because KeyValueStore will post to it
            GetSnapshotBlock = new ActionBlock<TaskCompletionSource<Snapshot>>(
                replyChannel =>
                {
                    var snapshot = core.GetSnapshot(OnSnapshotDisposed);
                    replyChannel.SetResult(snapshot);
                },
                new ExecutionDataflowBlockOptions { TaskScheduler = scheduler });

            // Unconstrained because OnSnapshotDisposed will post to it
            _snapshotDisposedBlock = new ActionBlock<Snapshot>(
                snapshot => core.OnSnapshotDisposedAsync(snapshot),
                new ExecutionDataflowBlockOptions { TaskScheduler = scheduler });
        }

        public ITargetBlock<SnapshotsUpdateMessage> SnapshotsUpdateBlock { get; }
        public ITargetBlock<TaskCompletionSource<Snapshot>> GetSnapshotBlock { get; }

        public Task Completion => Task.WhenAll(
            SnapshotsUpdateBlock.Completion, GetSnapshotBlock.Completion, _snapshotDisposedBlock.Completion);

        public void Complete()
        {
            SnapshotsUpdateBlock.Complete();
            GetSnapshotBlock.Complete();
            _snapshotDisposedBlock.Complete();
        }

        private void OnSnapshotDisposed(Snapshot snapshot)
        {
            var accepted = _snapshotDisposedBlock.Post(snapshot);
            Debug.Assert(accepted, "Posting a snapshot disposal message must always succeed.");
        }
    }
}

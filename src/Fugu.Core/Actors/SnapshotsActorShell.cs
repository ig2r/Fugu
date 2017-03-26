using Fugu.Channels;
using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class SnapshotsActorShell : ISnapshotsActor
    {
        private readonly SnapshotsActorCore _core;
        private readonly Channel<SnapshotsUpdateMessage> _snapshotsUpdateChannel;
        private readonly Channel<TaskCompletionSource<Snapshot>> _getSnapshotChannel;
        private readonly Channel<Snapshot> _snapshotDisposedChannel;

        public SnapshotsActorShell(
            SnapshotsActorCore core,
            Channel<SnapshotsUpdateMessage> snapshotsUpdateChannel,
            Channel<TaskCompletionSource<Snapshot>> getSnapshotChannel)
        {
            Guard.NotNull(core, nameof(core));
            Guard.NotNull(snapshotsUpdateChannel, nameof(snapshotsUpdateChannel));
            Guard.NotNull(getSnapshotChannel, nameof(getSnapshotChannel));

            _core = core;
            _snapshotsUpdateChannel = snapshotsUpdateChannel;
            _getSnapshotChannel = getSnapshotChannel;
            _snapshotDisposedChannel = new UnbufferedChannel<Snapshot>();
        }

        public async void Run()
        {
            await new SelectBuilder()
                .Case(_snapshotsUpdateChannel, msg =>
                {
                    return _core.UpdateIndexAsync(msg.Clock, msg.Index, msg.ReplyChannel);
                })
                .Case(_getSnapshotChannel, replyChannel =>
                {
                    var snapshot = _core.GetSnapshot(OnSnapshotDisposed);
                    replyChannel.SetResult(snapshot);
                    return Task.CompletedTask;
                })
                .Case(_snapshotDisposedChannel, snapshot =>
                {
                    return _core.OnSnapshotDisposedAsync(snapshot);
                })
                .SelectAsync(_ => true);
        }

        private void OnSnapshotDisposed(Snapshot snapshot)
        {
            _snapshotDisposedChannel.SendAsync(snapshot);
        }
    }
}

using Fugu.Actors;
using Fugu.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public class FastForwardLoadVisitor : TableVisitorBase
    {
        private readonly Segment _segment;
        private readonly IIndexActor _indexActor;

        public FastForwardLoadVisitor(Segment segment, IIndexActor indexActor)
        {
            Guard.NotNull(segment, nameof(segment));
            Guard.NotNull(indexActor, nameof(indexActor));

            _segment = segment;
            _indexActor = indexActor;
        }

        public Task Completion { get; private set; } = Task.CompletedTask;

        #region TableVisitorBase overrides

        public override void OnCommit(IEnumerable<byte[]> tombstones, IEnumerable<ParsedPutRecord> puts, ulong commitChecksum)
        {
            var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>();

            indexUpdates.AddRange(from key in tombstones
                                  select new KeyValuePair<byte[], IndexEntry>(
                                      key,
                                      new IndexEntry.Tombstone(_segment)));

            indexUpdates.AddRange(from put in puts
                                  select new KeyValuePair<byte[], IndexEntry>(
                                      put.Key,
                                      new IndexEntry.Value(_segment, put.ValueOffset, put.ValueLength)));

            var replyChannel = new TaskCompletionSource<VoidTaskResult>();
            Completion = replyChannel.Task;
            _indexActor.UpdateIndex(new StateVector(), indexUpdates, replyChannel);
        }

        #endregion
    }
}

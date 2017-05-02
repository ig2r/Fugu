using Fugu.Actors;
using Fugu.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Bootstrapping
{
    public class FastForwardLoadVisitor : TableVisitorBase
    {
        private readonly Segment _segment;
        private readonly ITargetBlock<UpdateIndexMessage> _indexUpdateBlock;

        public FastForwardLoadVisitor(Segment segment, ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
        {
            Guard.NotNull(segment, nameof(segment));
            Guard.NotNull(indexUpdateBlock, nameof(indexUpdateBlock));

            _segment = segment;
            _indexUpdateBlock = indexUpdateBlock;
        }

        public Task Completion { get; private set; } = Task.CompletedTask;

        #region TableVisitorBase overrides

        public override async Task OnCommitAsync(IEnumerable<byte[]> tombstones, IEnumerable<ParsedPutRecord> puts, ulong commitChecksum)
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

            await _indexUpdateBlock.SendAsync(new UpdateIndexMessage(new StateVector(), indexUpdates, replyChannel));
        }

        #endregion
    }
}

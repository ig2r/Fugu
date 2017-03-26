using Fugu.Actors;
using Fugu.Channels;
using Fugu.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public class FastForwardLoadVisitor : TableVisitorBase
    {
        private readonly Segment _segment;
        private readonly Channel<UpdateIndexMessage> _indexUpdateChannel;

        public FastForwardLoadVisitor(Segment segment, Channel<UpdateIndexMessage> indexUpdateChannel)
        {
            Guard.NotNull(segment, nameof(segment));
            Guard.NotNull(indexUpdateChannel, nameof(indexUpdateChannel));

            _segment = segment;
            _indexUpdateChannel = indexUpdateChannel;
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

            await _indexUpdateChannel.SendAsync(new UpdateIndexMessage(new StateVector(), indexUpdates, replyChannel));
        }

        #endregion
    }
}

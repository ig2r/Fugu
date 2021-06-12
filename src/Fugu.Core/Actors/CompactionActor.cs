using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    /// <summary>
    /// Inspects segment stats and if used/total ratio falls below a given threshold, merges a range
    /// of segments into a combined segment. When done compacting a range of segments, it will:
    /// - step the "Compaction" component of the vector clock
    /// - submit entries in the target segment for indexing (with baseline clock to ensure compaction results
    ///   don't overwrite index entries that were changed concurrently
    /// - hold all future compactions until it receives an updated index + stats that matches or is more recent
    ///   than that vector clock (to ensure that the received data reflects the compaction before starting another).
    ///   
    /// TBD: how to ensure source segments for a compaction are not deleted concurrently? Maybe drive eviction
    /// from this actor, too. Needs to receive "horizon" cutoff vector clock from SnapshotsActor then.
    /// </summary>
    public class CompactionActor
    {
        private readonly ChannelReader<byte> _segmentStatsUpdatedChannelReader;
        private readonly ChannelReader<byte> _segmentEmptiedChannelReader;
        private readonly ChannelReader<byte> _snapshotsUpdatedChannelReader;
        private readonly ChannelWriter<byte> _updateIndexChannelWriter;
        private readonly ChannelWriter<byte> _segmentEvictedChannelWriter;

        public CompactionActor(
            ChannelReader<byte> segmentStatsUpdatedChannelReader,
            ChannelReader<byte> segmentEmptiedChannelReader,
            ChannelReader<byte> snapshotsUpdatedChannelReader,
            ChannelWriter<byte> updateIndexChannelWriter,
            ChannelWriter<byte> segmentEvictedChannelWriter)
        {
            _segmentStatsUpdatedChannelReader = segmentStatsUpdatedChannelReader;
            _segmentEmptiedChannelReader = segmentEmptiedChannelReader;
            _snapshotsUpdatedChannelReader = snapshotsUpdatedChannelReader;
            _updateIndexChannelWriter = updateIndexChannelWriter;
            _segmentEvictedChannelWriter = segmentEvictedChannelWriter;
        }

        public Task ExecuteAsync()
        {
            return Task.CompletedTask;
        }
    }
}

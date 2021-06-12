using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    /// <summary>
    /// Maintains an immutable index of key-value pairs across all segments. Emits:
    /// - Updated index to SnaphotsActor
    /// - Updated index + changes to SegmentStatsActor
    /// </summary>
    public class IndexActor
    {
        private readonly ChannelReader<byte> _updateIndexChannelReader;
        private readonly ChannelWriter<byte> _indexUpdatedChannelWriter;
        private readonly ChannelWriter<byte> _updateSegmentStatsChannelWriter;

        public IndexActor(
            ChannelReader<byte> updateIndexChannelReader,
            ChannelWriter<byte> indexUpdatedChannelWriter,
            ChannelWriter<byte> updateSegmentStatsChannelWriter)
        {
            _updateIndexChannelReader = updateIndexChannelReader;
            _indexUpdatedChannelWriter = indexUpdatedChannelWriter;
            _updateSegmentStatsChannelWriter = updateSegmentStatsChannelWriter;
        }

        public async Task ExecuteAsync()
        {
            while (await _updateIndexChannelReader.WaitToReadAsync())
            {
                var message = await _updateIndexChannelReader.ReadAsync();
            }
        }
    }
}

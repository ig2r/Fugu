using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu
{
    /// <summary>
    /// Assigns a WriteBatch to a sufficiently-sized segment. Keeps track of:
    /// - the number of current segments (OR: total size of all segments?), to determine min size for the next;
    /// - remaining space within the current segment
    /// </summary>
    public class AllocationActor
    {
        private readonly ChannelReader<AllocationActorMessage> _input;
        private int _segments = 0;

        public AllocationActor(ChannelReader<AllocationActorMessage> input)
        {
            _input = input;
        }

        public async Task ExecuteAsync()
        {
            while (await _input.WaitToReadAsync())
            {
                switch (await _input.ReadAsync())
                {
                    case AllocationActorMessage.AllocateWriteBatch allocate:
                        _segments++;
                        break;
                    case AllocationActorMessage.NotifySegmentEvicted:
                        break;
                }
            }
        }
    }
}

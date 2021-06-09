using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu
{
    public class WriterActor
    {
        private readonly ChannelReader<WriteBatch> _input;

        public WriterActor(ChannelReader<WriteBatch> input)
        {
            _input = input;
        }

        public async Task ExecuteAsync()
        {
            while (true)
            {
                var batch = await _input.ReadAsync();
            }
        }
    }
}

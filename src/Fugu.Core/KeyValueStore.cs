using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu
{
    public class KeyValueStore
    {
        private readonly Channel<WriteBatch> _inputChannel;
        private readonly WriterActor _writerActor;

        // TODO: make ctor private, have CreateAsync static method instead
        public KeyValueStore()
        {
            _inputChannel = Channel.CreateUnbounded<WriteBatch>();
            _writerActor = new WriterActor(_inputChannel.Reader);

            _ = _writerActor.ExecuteAsync();
        }

        public ValueTask<Snapshot> GetSnapshotAsync()
        {
            return ValueTask.FromResult(new Snapshot());
        }

        public ValueTask WriteAsync(WriteBatch batch)
        {
            return _inputChannel.Writer.WriteAsync(batch);
        }
    }
}

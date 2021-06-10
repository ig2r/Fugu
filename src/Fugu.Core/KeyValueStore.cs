using System;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu
{
    public class KeyValueStore : IAsyncDisposable
    {
        private readonly Channel<WriteBatch> _inputChannel;
        private readonly WriterActor _writerActor;

        private KeyValueStore()
        {
            _inputChannel = Channel.CreateUnbounded<WriteBatch>();
            _writerActor = new WriterActor(_inputChannel.Reader);
        }

        public static Task<KeyValueStore> CreateAsync()
        {
            var store = new KeyValueStore();
            var done = Task.WhenAll(store._writerActor.ExecuteAsync());
            return Task.FromResult(store);
        }

        public ValueTask<Snapshot> GetSnapshotAsync()
        {
            return ValueTask.FromResult(new Snapshot());
        }

        public ValueTask WriteAsync(WriteBatch batch)
        {
            return _inputChannel.Writer.WriteAsync(batch);
        }

        public ValueTask DisposeAsync()
        {
            _inputChannel.Writer.Complete();
            return ValueTask.CompletedTask;
        }
    }
}

using System;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu
{
    public class KeyValueStore : IAsyncDisposable
    {
        private readonly Channel<AllocationActorMessage> _allocationActorInput;
        private readonly Channel<WriteBatch> _inputChannel;
        
        private readonly AllocationActor _allocationActor;
        private readonly WriterActor _writerActor;

        private KeyValueStore()
        {
            _allocationActorInput = Channel.CreateUnbounded<AllocationActorMessage>();
            _inputChannel = Channel.CreateUnbounded<WriteBatch>();

            _allocationActor = new AllocationActor(_allocationActorInput.Reader);
            _writerActor = new WriterActor(_inputChannel.Reader);
        }

        public static Task<KeyValueStore> CreateAsync()
        {
            var store = new KeyValueStore();
            _ = Task.WhenAll(
                store._allocationActor.ExecuteAsync(),
                store._writerActor.ExecuteAsync());

            return Task.FromResult(store);
        }

        public ValueTask<Snapshot> GetSnapshotAsync()
        {
            return ValueTask.FromResult(new Snapshot());
        }

        public ValueTask WriteAsync(WriteBatch batch)
        {
            return _allocationActorInput.Writer.WriteAsync(
                new AllocationActorMessage.AllocateWriteBatch(batch));
        }

        public ValueTask DisposeAsync()
        {
            _inputChannel.Writer.Complete();
            return ValueTask.CompletedTask;
        }
    }
}

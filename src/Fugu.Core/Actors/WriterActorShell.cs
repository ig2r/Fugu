using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class WriterActorShell : IWriterActor
    {
        private readonly WriterActorCore _core;
        private readonly MessageLoop _loop = new MessageLoop();

        public WriterActorShell(WriterActorCore core)
        {
            Guard.NotNull(core, nameof(core));
            _core = core;
        }

        #region IWriterActor

        public async void Commit(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            using (await _loop.WaitAsync())
            {
                _core.Commit(writeBatch, replyChannel);
            }
        }

        public async void StartNewSegment(IOutputTable outputTable)
        {
            using (await _loop.WaitAsync())
            {
                await _core.StartNewSegmentAsync(outputTable);
            }
        }

        #endregion
    }
}

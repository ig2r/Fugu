using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu.Messages
{
    public class AllocateWriteBatchMessage
    {
        public AllocateWriteBatchMessage(WriteBatch batch, TaskCompletionSource completionSource)
        {
            Batch = batch;
            CompletionSource = completionSource;
        }

        public WriteBatch Batch { get; }
        public TaskCompletionSource CompletionSource { get; }
    }
}

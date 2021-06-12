using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu.Messages
{
    public class WriteAllocatedBatchMessage
    {
        public WriteAllocatedBatchMessage(WriteBatch batch, Segment segment, TaskCompletionSource completionSource)
        {
            Batch = batch;
            Segment = segment;
            CompletionSource = completionSource;
        }

        public WriteBatch Batch { get; }
        public Segment Segment { get; }
        public TaskCompletionSource CompletionSource { get; }
    }
}

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Fugu.Common
{
    // Semantics:
    // - senders and receivers are matched to facilitate data exchange
    // - multiple senders
    // - only one receiver
    public sealed class UnbufferedChannel<T> : Channel<T>
    {
        private readonly object _sync = new object();
        private readonly Queue<(TaskCompletionSource<VoidTaskResult> tcs, T item)> _blockedSenders =
            new Queue<(TaskCompletionSource<VoidTaskResult> tcs, T item)>();
        private TaskCompletionSource<T> _blockedReceiver;

        public override Task SendAsync(T item)
        {
            TaskCompletionSource<T> blockedReceiver;
            lock (_sync)
            {
                if (_blockedReceiver == null)
                {
                    var blockedSender = (tcs: new TaskCompletionSource<VoidTaskResult>(), item: item);
                    _blockedSenders.Enqueue(blockedSender);
                    return blockedSender.tcs.Task;
                }

                blockedReceiver = _blockedReceiver;
                _blockedReceiver = null;
            }

            blockedReceiver.SetResult(item);
            return Task.CompletedTask;
        }

        public override ValueTask<T> ReceiveAsync()
        {
            (TaskCompletionSource<VoidTaskResult> tcs, T item) blockedSender;
            lock (_sync)
            {
                if (_blockedSenders.Count == 0)
                {
                    _blockedReceiver = new TaskCompletionSource<T>();
                    return new ValueTask<T>(_blockedReceiver.Task);
                }

                blockedSender = _blockedSenders.Dequeue();
            }

            blockedSender.tcs.SetResult(default(VoidTaskResult));
            return new ValueTask<T>(blockedSender.item);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Fugu.Common
{
    public sealed class UnboundedChannel<T> : Channel<T>
    {
        private readonly object _sync = new object();
        private readonly Queue<T> _items = new Queue<T>();
        private TaskCompletionSource<T> _blockedReceiver;

        public override Task SendAsync(T item)
        {
            TaskCompletionSource<T> receiverToRelease = null;
            lock (_sync)
            {
                if (_blockedReceiver != null)
                {
                    receiverToRelease = _blockedReceiver;
                    _blockedReceiver = null;
                }
                else
                {
                    _items.Enqueue(item);
                }
            }

            if (receiverToRelease != null)
            {
                receiverToRelease.SetResult(item);
            }

            return Task.CompletedTask;
        }

        public override ValueTask<T> ReceiveAsync()
        {
            T item;
            lock (_sync)
            {
                if (_items.Count > 0)
                {
                    item = _items.Dequeue();
                }
                else
                {
                    _blockedReceiver = new TaskCompletionSource<T>();
                    return new ValueTask<T>(_blockedReceiver.Task);
                }
            }

            return new ValueTask<T>(item);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Fugu.Channels
{
    // Semantics:
    // - senders and receivers are matched to facilitate data exchange
    // - multiple senders
    // - only one receiver
    public sealed class UnbufferedChannel<T> : Channel<T>
    {
        private readonly SemaphoreSlim _receiverCount = new SemaphoreSlim(0, 1);
        private AsyncValueTaskMethodBuilder<T> _blockedReceiver;

        public override async Task SendAsync(T item)
        {
            await _receiverCount.WaitAsync().ConfigureAwait(false);
            _blockedReceiver.SetResult(item);
        }

        public override ValueTask<T> ReceiveAsync()
        {
            _blockedReceiver = AsyncValueTaskMethodBuilder<T>.Create();
            _receiverCount.Release();
            return _blockedReceiver.Task;
        }
    }
}

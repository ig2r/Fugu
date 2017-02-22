using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Fugu.Common
{
    /// <summary>
    /// An asynchronous lock with first-in, first-out (FIFO) semantics. Useful for actor-like concurrency.
    /// </summary>
    public class MessageLoop
    {
        private readonly object _sync = new object();
        private readonly Queue<TaskCompletionSource<Releaser>> _waiters = new Queue<TaskCompletionSource<Releaser>>();

        private int _inflight = 0;
        private int _currentTicket = 1;

        public ValueTask<Releaser> WaitAsync()
        {
            lock (_sync)
            {
                if (++_inflight == 1)
                {
                    // Uncontended, access immediately
                    var releaser = new Releaser(this, _currentTicket);
                    return new ValueTask<Releaser>(releaser);
                }
                else
                {
                    // Enqueue
                    var tcs = new TaskCompletionSource<Releaser>();
                    _waiters.Enqueue(tcs);
                    return new ValueTask<Releaser>(tcs.Task);
                }
            }
        }

        private void Release(int ticket)
        {
            TaskCompletionSource<Releaser> dequeuedWaiter = null;

            lock (_sync)
            {
                // Ignore attempts to release a stale ticket
                if (_currentTicket != ticket)
                {
                    return;
                }

                // Invalidate ticket
                ++_currentTicket;

                // If more operations are waiting, start the next one
                if (--_inflight > 0)
                {
                    dequeuedWaiter = _waiters.Dequeue();
                }
            }

            // Avoid running SetResult under the lock
            if (dequeuedWaiter != null)
            {
                var releaser = new Releaser(this, _currentTicket);
                dequeuedWaiter.SetResult(releaser);
            }
        }

        public struct Releaser : IDisposable
        {
            private readonly MessageLoop _loop;
            private readonly int _ticket;

            internal Releaser(MessageLoop loop, int ticket)
            {
                _loop = loop;
                _ticket = ticket;
            }

            #region IDisposable

            public void Dispose()
            {
                _loop.Release(_ticket);
            }

            #endregion
        }
    }
}

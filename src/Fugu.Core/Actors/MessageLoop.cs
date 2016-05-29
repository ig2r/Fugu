using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class MessageLoop
    {
        private readonly TaskScheduler _targetScheduler = TaskScheduler.Default;
        private readonly BlockingCollection<Action> _queuedActions = new BlockingCollection<Action>();
        private int _waiters = 0;

        public Awaiter GetAwaiter()
        {
            if (Interlocked.Increment(ref _waiters) == 1)
            {
                // No one was waiting so far, can start processing immediately
                return new Awaiter(this, true);
            }
            else
            {
                // The caller will have to wait in line
                return new Awaiter(this, false);
            }
        }

        private void AddContinuation(Action continuation)
        {
            _queuedActions.Add(continuation);
        }

        private void Release()
        {
            if (Interlocked.Decrement(ref _waiters) > 0)
            {
                var continuation = _queuedActions.Take();

                if (TaskScheduler.Current == _targetScheduler)
                {
                    continuation();
                }
                else
                {
                    Task.Factory.StartNew(continuation, CancellationToken.None, TaskCreationOptions.DenyChildAttach, _targetScheduler);
                }
            }
        }

        #region Nested types

        public struct Awaiter : INotifyCompletion
        {
            private readonly MessageLoop _messageLoop;
            private readonly bool _isCompleted;

            public Awaiter(MessageLoop messageLoop, bool isCompleted)
            {
                _messageLoop = messageLoop;
                _isCompleted = isCompleted;
            }

            public bool IsCompleted
            {
                get { return _isCompleted; }
            }

            public Releaser GetResult()
            {
                // TODO: block synchronously or throw exception if the loop is not yet available
                return new Releaser(_messageLoop);
            }

            public void OnCompleted(Action continuation)
            {
                Debug.Assert(!_isCompleted);
                _messageLoop.AddContinuation(continuation);
            }
        }

        public struct Releaser : IDisposable
        {
            private readonly MessageLoop _messageLoop;

            public Releaser(MessageLoop messageLoop)
            {
                _messageLoop = messageLoop;
            }

            public void Dispose()
            {
                _messageLoop.Release();
            }
        }

        #endregion
    }
}
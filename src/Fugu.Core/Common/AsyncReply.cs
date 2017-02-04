using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Fugu.Common
{
    /// <summary>
    /// A thin union over tasks of type <typeparamref name="T"/> and immediately-available
    /// results of <typeparamref name="T"/>. This type exists to prevent an extra allocation
    /// of a <see cref="Task"/> instance when a return value is available synchronously.
    /// </summary>
    /// <typeparam name="T">Result type of the represented operation.</typeparam>
    public struct AsyncReply<T>
    {
        private readonly T _result;
        private readonly Task<T> _task;

        public AsyncReply(T result)
        {
            _result = result;
            _task = null;
        }

        public AsyncReply(Task<T> task)
        {
            Guard.NotNull(task, nameof(task));

            _result = default(T);
            _task = task;
        }

        public bool IsCompleted => _task == null || _task.IsCompleted;

        public Task<T> AsTask()
        {
            return _task ?? Task.FromResult(_result);
        }

        public Awaiter GetAwaiter()
        {
            return new Awaiter(this);
        }

        public struct Awaiter : ICriticalNotifyCompletion
        {
            private readonly AsyncReply<T> _reply;

            public Awaiter(AsyncReply<T> reply)
            {
                _reply = reply;
            }

            public bool IsCompleted => _reply.IsCompleted;

            #region ICriticalNotifyCompletion

            public void OnCompleted(Action continuation)
            {
                _reply._task.GetAwaiter().OnCompleted(continuation);
            }

            public void UnsafeOnCompleted(Action continuation)
            {
                _reply._task.GetAwaiter().UnsafeOnCompleted(continuation);
            }

            #endregion

            public T GetResult()
            {
                return _reply._task == null ? _reply._result : _reply._task.GetAwaiter().GetResult();
            }
        }
    }
}

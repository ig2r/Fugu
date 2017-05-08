using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Common
{
    public class Disposable : IDisposable
    {
        private bool _disposed;
        private readonly Action _action;

        public Disposable(Action action)
        {
            Guard.NotNull(action, nameof(action));
            _action = action;
        }

        #region IDisposable

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                _action();
            }
        }

        #endregion
    }
}

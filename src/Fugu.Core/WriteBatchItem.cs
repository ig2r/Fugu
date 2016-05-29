using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu
{
    public abstract class WriteBatchItem
    {
        private WriteBatchItem()
        {
        }

        // Poor man's pattern matching
        public abstract void Match(Action<Put> onPut, Action<Delete> onDelete);
        public abstract TResult Match<TResult>(Func<Put, TResult> onPut, Func<Delete, TResult> onDelete);

        public sealed class Put : WriteBatchItem
        {
            public Put(byte[] value)
            {
                Guard.NotNull(value, nameof(value));
                Value = value;
            }

            public byte[] Value { get; }

            public override void Match(Action<Put> onPut, Action<Delete> onDelete)
            {
                onPut(this);
            }

            public override T Match<T>(Func<Put, T> onPut, Func<Delete, T> onDelete)
            {
                return onPut(this);
            }
        }

        public sealed class Delete : WriteBatchItem
        {
            public override void Match(Action<Put> onPut, Action<Delete> onDelete)
            {
                onDelete(this);
            }

            public override T Match<T>(Func<Put, T> onPut, Func<Delete, T> onDelete)
            {
                return onDelete(this);
            }
        }
    }
}

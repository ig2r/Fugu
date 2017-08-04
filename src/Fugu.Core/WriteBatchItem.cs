using Fugu.Common;

namespace Fugu
{
    /// <summary>
    /// Represents a pending put or delete operation.
    /// </summary>
    public abstract class WriteBatchItem
    {
        private WriteBatchItem()
        {
        }

        public sealed class Put : WriteBatchItem
        {
            public Put(byte[] value)
            {
                Guard.NotNull(value, nameof(value));
                Value = value;
            }

            public byte[] Value { get; }
        }

        public sealed class Delete : WriteBatchItem
        {
        }
    }
}

using System;
using System.Text;

namespace Fugu
{
    public static class WriteBatchExtensions
    {
        public static WriteBatch Add(this WriteBatch batch, string key, ReadOnlyMemory<byte> value)
        {
            return batch.Add(Encoding.UTF8.GetBytes(key), value);
        }

        public static WriteBatch Remove(this WriteBatch batch, string key)
        {
            return batch.Remove(Encoding.UTF8.GetBytes(key));
        }
    }
}

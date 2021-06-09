using System;
using System.Buffers;
using System.Text;

namespace Fugu
{
    public class WriteBatch
    {
        // - Use "Add" for compatibility with list-style initializers. Need to implement IEnumerable, though.
        // - Use ReadOnlyMemory<byte> instead of byte[] for compatibility reasons - perf impact?
        // - Offer WriteBatch extension methods to allow Add() with string keys, etc.
        public WriteBatch Add(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
        {
            return this;
        }
        // Add/Remove naming consistent with List<T>, Dictionary<T, U>, etc.
        // BUT: note that Dictionary<T, U>.Add will throw if given key already exists... so, Set()? Put()?
        public WriteBatch Remove(ReadOnlyMemory<byte> key)
        {
            return this;
        }

        public ReadOnlyMemory<byte> this[ReadOnlyMemory<byte> key]
        {
            get => default;
            set { }
        }

        public ReadOnlyMemory<byte> this[string key]
        {
            get => this[Encoding.UTF8.GetBytes(key)];
            set { }
        }
    }
}

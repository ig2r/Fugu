using System;

namespace Fugu
{
    public class WriteBatch
    {
        // - Use "Add" for compatibility with dictionary-style initializers. Need to implement IEnumerable, though.
        // - Use ReadOnlyMemory<byte> instead of byte[] for compatibility reasons - perf impact?
        // - Offer WriteBatch extension methods to allow Add() with string keys, etc.
        public void Add(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
        {
        }
    }
}

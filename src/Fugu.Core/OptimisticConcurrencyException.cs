using System;

namespace Fugu
{
    public sealed class OptimisticConcurrencyException : InvalidOperationException
    {
        public OptimisticConcurrencyException(byte[] key)
            : base($"Detected concurrent modifcation of key {key}.")
        {
            Key = key;
        }

        public byte[] Key { get; }
    }
}

using Fugu.Common;
using Fugu.Format;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public struct ParsedPutRecord
    {
        public ParsedPutRecord(byte[] key, long valueOffset, int valueLength)
        {
            Guard.NotNull(key, nameof(key));

            Key = key;
            ValueOffset = valueOffset;
            ValueLength = valueLength;
        }

        public byte[] Key { get; }
        public long ValueOffset { get; }
        public int ValueLength { get; }
    }
}

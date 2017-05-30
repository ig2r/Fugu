using System;
using System.Diagnostics;

namespace Fugu.Common
{
    /// <summary>
    /// Represents an integer range.
    /// </summary>
    [DebuggerDisplay("Offset = {Offset}, Count = {Count}")]
    public struct Range
    {
        public Range(int offset, int count)
        {
            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }

            Offset = offset;
            Count = count;
        }

        public int Offset { get; }
        public int Count { get; }
    }
}

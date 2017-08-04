using System;

namespace Fugu
{
    public interface ITable
    {
        long Capacity { get; }
        Span<byte> GetSpan(long offset);
    }
}

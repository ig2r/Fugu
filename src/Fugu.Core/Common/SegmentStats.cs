using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Common
{
    [DebuggerDisplay("LiveBytes = {LiveBytes}, DeadBytes = {DeadBytes}")]
    public sealed class SegmentStats
    {
        public long LiveBytes { get; private set; } = 0;
        public long DeadBytes { get; private set; } = 0;
        public long TotalBytes => LiveBytes + DeadBytes;

        public void AddLiveBytes(long count)
        {
            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }

            LiveBytes += count;
        }

        public void MarkBytesAsDead(long count)
        {
            if (count > LiveBytes)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }

            LiveBytes -= count;
            DeadBytes += count;
        }
    }
}

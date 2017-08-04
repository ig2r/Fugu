using System;
using System.Diagnostics;

namespace Fugu.Actors
{
    /// <summary>
    /// Usage statistics for an associated segment, i.e., the number of "live" and "dead" payload bytes within.
    /// </summary>
    [DebuggerDisplay("LiveBytes = {LiveBytes}, DeadBytes = {DeadBytes}")]
    public struct SegmentStats
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SegmentStats"/> struct.
        /// </summary>
        /// <param name="liveBytes">The number of bytes in the associated segment that describe current data.</param>
        /// <param name="deadBytes">The number of bytes in the associated segment that no longer describe current data.</param>
        public SegmentStats(long liveBytes, long deadBytes)
        {
            if (liveBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(liveBytes));
            }

            if (deadBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(deadBytes));
            }

            LiveBytes = liveBytes;
            DeadBytes = deadBytes;
        }

        /// <summary>
        /// Gets the number of bytes in the associated segment that describe current data.
        /// </summary>
        public long LiveBytes { get; }

        /// <summary>
        /// Gets the number of bytes in the associated segment that no longer describe current data.
        /// </summary>
        public long DeadBytes { get; }

        /// <summary>
        /// Gets the total number of bytes (live and dead) in the associated segment.
        /// </summary>
        public long TotalBytes => LiveBytes + DeadBytes;
    }
}

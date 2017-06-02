using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Bootstrapping
{
    public class SegmentLoadResult
    {
        public SegmentLoadResult(Segment segment, bool hasValidFooter, long lastGoodPosition)
        {
            Guard.NotNull(segment, nameof(segment));
            Segment = segment;
            HasValidFooter = hasValidFooter;
            LastGoodPosition = lastGoodPosition;
        }

        public Segment Segment { get; }
        public bool HasValidFooter { get; }
        public long LastGoodPosition { get; }
    }
}

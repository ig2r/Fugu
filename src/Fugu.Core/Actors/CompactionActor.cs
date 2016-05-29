using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class CompactionActor
    {
        private readonly MessageLoop _messageLoop = new MessageLoop();

        public CompactionActor()
        {

        }

        public async void OnCompactableSegmentsChanged(IReadOnlyList<SegmentStats> segmentStats)
        {
            using (await _messageLoop)
            {

            }
        }
    }
}

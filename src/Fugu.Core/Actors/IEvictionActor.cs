using Fugu.Common;

namespace Fugu.Actors
{
    public interface IEvictionActor
    {
        void ScheduleEviction(VectorClock evictAt, Segment segment);
    }
}
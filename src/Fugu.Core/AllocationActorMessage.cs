namespace Fugu
{
    public class AllocationActorMessage
    {
        private AllocationActorMessage()
        {
        }

        public sealed class AllocateWriteBatch : AllocationActorMessage
        {
            public AllocateWriteBatch(WriteBatch batch)
            {
                Batch = batch;
            }

            public WriteBatch Batch { get; }
        }

        public sealed class NotifySegmentEvicted : AllocationActorMessage
        {
        }
    }
}

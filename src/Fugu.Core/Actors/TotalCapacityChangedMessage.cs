namespace Fugu.Actors
{
    /// <summary>
    /// Signals that the total capacity of the store's table set has changed. This occurs when a table is evicted
    /// from the store, or a new compacted table is added.
    /// </summary>
    public struct TotalCapacityChangedMessage
    {
        public TotalCapacityChangedMessage(long deltaCapacity)
        {
            DeltaCapacity = deltaCapacity;
        }

        public long DeltaCapacity { get; }
    }
}

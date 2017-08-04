namespace Fugu.IO.Records
{
    /// <summary>
    /// Used to distinguish between <see cref="PutRecord"/> and <see cref="TombstoneRecord"/> records.
    /// </summary>
    public enum CommitRecordType : byte
    {
        Put = 0x01,
        Tombstone = 0x02,
    }
}

namespace Fugu.IO.Records
{
    /// <summary>
    /// Used to distinguish between <see cref="CommitHeader"/> and <see cref="TableFooter"/> records.
    /// </summary>
    public enum TableRecordType : byte
    {
        CommitHeader = 0x01,
        TableFooter = 0x02,
    }
}

namespace Fugu.DataFormat
{
    public enum RecordType : byte
    {
        Put = 0x01,
        Tombstone = 0x02,
        StartData = 0x03,
        Commit = 0x04,
    }
}

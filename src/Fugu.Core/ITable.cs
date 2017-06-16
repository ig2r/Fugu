using Fugu.Format;

namespace Fugu
{
    public interface ITable
    {
        long Capacity { get; }
        TableReader GetReader(long position, long size);
    }
}

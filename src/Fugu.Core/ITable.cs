using System.IO;

namespace Fugu
{
    public interface ITable
    {
        long Capacity { get; }
        Stream GetInputStream(long position, long size);
        Stream GetOutputStream(long position, long size);
    }
}

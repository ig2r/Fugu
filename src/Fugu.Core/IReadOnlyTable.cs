using System.IO;

namespace Fugu
{
    public interface IReadOnlyTable
    {
        long Capacity { get; }
        Stream GetInputStream(long position, long size);
    }
}

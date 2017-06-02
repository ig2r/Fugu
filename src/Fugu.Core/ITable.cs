using System.IO;

namespace Fugu
{
    public interface ITable : IReadOnlyTable
    {
        Stream GetOutputStream(long position, long size);
    }
}

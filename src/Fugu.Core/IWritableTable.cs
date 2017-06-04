using System.IO;

namespace Fugu
{
    public interface IWritableTable : ITable
    {
        Stream GetOutputStream(long position, long size);
    }
}

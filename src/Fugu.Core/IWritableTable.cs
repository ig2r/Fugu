using Fugu.Format;
using System.IO;

namespace Fugu
{
    public interface IWritableTable : ITable
    {
        TableWriter GetWriter();
    }
}

using System.Threading.Tasks;

namespace Fugu
{
    public interface ITableFactory
    {
        Task<IWritableTable> CreateTableAsync(long capacity);
    }
}

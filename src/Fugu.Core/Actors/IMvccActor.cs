using System.Threading.Tasks;

namespace Fugu.Actors
{
    public interface IMvccActor
    {
        Task CommitAsync(WriteBatch batch);
    }
}
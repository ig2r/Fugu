using System.Threading.Tasks;

namespace Fugu.Common
{
    public abstract class Channel<T>
    {
        public abstract Task SendAsync(T item);
        public abstract ValueTask<T> ReceiveAsync();
    }
}

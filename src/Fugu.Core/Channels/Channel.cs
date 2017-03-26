using System.Threading.Tasks;

namespace Fugu.Channels
{
    public abstract class Channel<T>
    {
        public abstract Task SendAsync(T item);
        public abstract ValueTask<T> ReceiveAsync();
    }
}

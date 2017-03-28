using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Fugu.Common
{
    public class SelectBuilder
    {
        private readonly List<Func<SemaphoreSlim, Task>> _cases = new List<Func<SemaphoreSlim, Task>>();

        public SelectBuilder Case<T>(Channel<T> channel, Func<T, Task> func)
        {
            Guard.NotNull(channel, nameof(channel));
            Guard.NotNull(func, nameof(func));

            _cases.Add(async semaphore =>
            {
                var item = await channel.ReceiveAsync();
                await semaphore.WaitAsync();
                try
                {
                    await func(item);
                }
                finally
                {
                    semaphore.Release();
                }
            });

            return this;
        }

        public async Task SelectAsync(Func<int, bool> predicate)
        {
            using (var semaphore = new SemaphoreSlim(1, 1))
            {
                var i = 0;
                var tasks = _cases.Select(c => c(semaphore)).ToList();

                do
                {
                    var completedTask = await Task.WhenAny(tasks);
                    var index = tasks.IndexOf(completedTask);
                    tasks[index] = _cases[index](semaphore);
                }
                while (predicate(++i));
            }
        }
    }
}

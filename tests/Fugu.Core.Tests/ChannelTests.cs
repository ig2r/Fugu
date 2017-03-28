using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Fugu.Core.Tests
{
    public class ChannelTests
    {
        [Fact]
        public void ReceiveAsync_EmptyChannel_ReturnsUnfinishedTask()
        {
            var chan = new UnbufferedChannel<int>();
            var task = chan.ReceiveAsync();

            Assert.False(task.IsCompletedSuccessfully);
        }

        [Fact]
        public async Task ReceiveAsync_ChannelHasOneItem_ReturnsItem()
        {
            var chan = new UnbufferedChannel<int>();

            var sendTask = chan.SendAsync(1);
            var receiveTask = chan.ReceiveAsync();

            var item = await receiveTask;

            Assert.Equal(1, item);
        }
    }
}

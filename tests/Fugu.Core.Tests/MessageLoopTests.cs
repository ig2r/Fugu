using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Fugu.Core.Tests
{
    public class MessageLoopTests
    {
        [Fact]
        public void WaitAsync_Uncontended_AcquiredSynchronously()
        {
            // Arrange:
            var loop = new MessageLoop();

            // Act:
            var waiter = loop.WaitAsync();

            // Assert:
            Assert.True(waiter.IsCompletedSuccessfully);
        }

        [Fact]
        public void WaitAsync_Contended_SecondRequestMustWait()
        {
            // Arrange:
            var loop = new MessageLoop();
            var firstWaiter = loop.WaitAsync();

            // Act:
            var secondWaiter = loop.WaitAsync();

            // Assert:
            Assert.False(secondWaiter.IsCompleted);
        }

        [Fact]
        public void WaitAsync_FirstRequestCompleted_SecondRequestMayStart()
        {
            // Arrange:
            var loop = new MessageLoop();
            var firstWaiter = loop.WaitAsync();
            var secondWaiter = loop.WaitAsync();

            // Act:
            firstWaiter.Result.Dispose();

            // Assert:
            Assert.True(secondWaiter.IsCompletedSuccessfully);
        }
    }
}

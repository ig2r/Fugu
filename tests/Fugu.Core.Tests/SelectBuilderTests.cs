using Fugu.Common;
using System.Threading.Tasks;
using Xunit;

namespace Fugu.Core.Tests
{
    public class SelectBuilderTests
    {
        [Fact]
        public async Task SelectAsync_TwoChannels_RunsBothHandlers()
        {
            // Arrange
            var chan1 = new UnbufferedChannel<int>();
            var chan2 = new UnbufferedChannel<string>();
            bool receivedFromChan1 = false;
            bool receivedFromChan2 = false;

            var sendTask1 = chan1.SendAsync(1);
            var sendTask2 = chan2.SendAsync("foo");

            var selectBuilder = new SelectBuilder()
                .Case(chan1, _ => { receivedFromChan1 = true; return Task.CompletedTask; })
                .Case(chan2, _ => { receivedFromChan2 = true; return Task.CompletedTask; });

            // Act
            await selectBuilder.SelectAsync(n => n < 2);

            // Assert
            Assert.True(receivedFromChan1);
            Assert.True(receivedFromChan2);
        }
    }
}

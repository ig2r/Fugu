using Fugu.Common;
using System;
using Xunit;

namespace Fugu.Core.Tests
{
    public class GuardTests
    {
        [Fact]
        public void NotNull_NonNullParameter_DoesNotThrow()
        {
            var param = new object();
            Guard.NotNull(param, nameof(param));
        }

        [Fact]
        public void NotNull_NullParameter_ThrowsArgumentNullException()
        {
            object param = null;
            var ex = Record.Exception(() => Guard.NotNull(param, nameof(param)));

            Assert.NotNull(ex);
            Assert.IsAssignableFrom<ArgumentNullException>(ex);
        }

        [Fact]
        public void NotNull_NullParameterAndNonNullParameterName_ExceptionHasParameterName()
        {
            object param = null;
            var paramName = nameof(param);

            var ex = (ArgumentNullException)Record.Exception(() => Guard.NotNull(param, paramName));

            Assert.Equal(paramName, ex.ParamName);
        }
    }
}

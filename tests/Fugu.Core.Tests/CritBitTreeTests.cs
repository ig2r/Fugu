using Fugu.Common;
using System.Linq;
using System.Text;
using Xunit;

namespace Fugu.Core.Tests
{
    using Tree = CritBitTree<ByteArrayKeyTraits, byte[], int>;

    public class CritBitTreeTests
    {
        private readonly byte[] _empty = new byte[0];
        private readonly byte[] _test = Encoding.UTF8.GetBytes("test");
        private readonly byte[] _toaster = Encoding.UTF8.GetBytes("toaster");
        private readonly byte[] _toasting = Encoding.UTF8.GetBytes("toasting");
        private readonly byte[] _slow = Encoding.UTF8.GetBytes("slow");
        private readonly byte[] _slowly = Encoding.UTF8.GetBytes("slowly");

        [Fact]
        public void Empty_IsNotNull()
        {
            // Arrange:
            // Act:
            // Assert:
            Assert.NotNull(Tree.Empty);
        }

        #region SetItem

        [Fact]
        public void SetItem_OnEmptyTree_ReturnsNonNullInstance()
        {
            // Arrange:
            var empty = Tree.Empty;

            // Act:
            var t = empty.SetItem(_empty, 1);

            // Assert:
            Assert.NotNull(t);
            Assert.NotSame(empty, t);
        }

        [Fact]
        public void SetItem_InsertingSameItemTwice_OverwritesExistingItem()
        {
            // Arrange:
            var t = Tree.Empty;
            t = t.SetItem(_test, 1);

            // Act:
            t = t.SetItem(_test, 2);

            // Assert:
            var items = t.GetItems(new byte[0]).Select(kvp => kvp.Key).ToArray();
            Assert.Equal(new[] { _test }, items);
        }

        [Fact]
        public void SetItem_FiveItems_InsertsAllItems()
        {
            // Arrange:
            var t = Tree.Empty;

            // Act:
            t = t.SetItem(_toasting, 1);
            t = t.SetItem(_slowly, 2);
            t = t.SetItem(_slow, 3);
            t = t.SetItem(_toaster, 4);
            t = t.SetItem(_test, 5);

            // Assert:
            var items = t.GetItems(new byte[0]).Select(kvp => kvp.Key).ToArray();
            Assert.Equal(new[] { _slow, _slowly, _test, _toaster, _toasting }, items);
        }

        #endregion

        #region GetItems

        [Fact]
        public void GetItems_WithStartKeyThatExistsInTree_ReturnsAllItemsIncludingSearchKey()
        {
            // Arrange:
            var t = Tree.Empty;
            t = t.SetItem(_toasting, 1);
            t = t.SetItem(_slowly, 2);
            t = t.SetItem(_slow, 3);
            t = t.SetItem(_toaster, 4);
            t = t.SetItem(_test, 5);

            // Act:
            var items = t.GetItems(_test).Select(kvp => kvp.Key).ToArray();

            // Assert:
            Assert.Equal(new[] { _test, _toaster, _toasting }, items);
        }

        [Fact]
        public void GetItems_WithStartKeyThatDoesNotExistInTree_ReturnsAllSuccessors()
        {
            // Arrange:
            var t = Tree.Empty;
            t = t.SetItem(_toasting, 1);
            t = t.SetItem(_slowly, 2);
            t = t.SetItem(_slow, 3);
            t = t.SetItem(_toaster, 4);
            t = t.SetItem(_test, 5);

            // Act:
            var text = Encoding.UTF8.GetBytes("text");
            var items = t.GetItems(text).Select(kvp => kvp.Key).ToArray();

            // Assert:
            Assert.Equal(new[] { _toaster, _toasting }, items);
        }
        #endregion

        #region ByteArrayKeyTraits.GetBit

        [Theory]
        [InlineData(new byte[0], new[] { 0 })]
        [InlineData(new byte[] { 0x00 }, new[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0 })]
        [InlineData(new byte[] { 0xFF }, new[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 0 })]
        [InlineData(new byte[] { 0x43 }, new[] { 1, 0, 1, 0, 0, 0, 0, 1, 1, 0 })]
        [InlineData(new byte[] { 0xFF, 0xFF }, new[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0 })]
        public void GetBit_ForValidBitIndices_ReturnsExpectedRepresentations(byte[] key, int[] expectedBits)
        {
            // Arrange:
            var traits = new ByteArrayKeyTraits();

            // Act:
            var actualBits = Enumerable.Range(0, expectedBits.Length).Select(i => traits.GetBit(key, i)).ToArray();

            // Assert:
            Assert.Equal(expectedBits, actualBits);
        }

        #endregion

        #region ByteArrayKeyTraits.FindCritBitIndex

        [Theory]
        [InlineData(new byte[0], new byte[0], int.MaxValue)]
        [InlineData(new byte[0], new byte[] { 0xFF }, 0)]
        [InlineData(new byte[0], new byte[] { 0x00 }, 0)]
        [InlineData(new byte[] { 0x55, 0xF7 }, new byte[] { 0x55, 0xF0, 0xFF }, 15)]
        public void FindCritBitIndex_ForValidData_ReturnsExpectedResults(byte[] key1, byte[] key2, int expectedBitIndex)
        {
            // Arrange:
            var traits = new ByteArrayKeyTraits();

            // Act:
            var actualBitIndex = traits.FindCritBitIndex(key1, key2);

            // Assert:
            Assert.Equal(expectedBitIndex, actualBitIndex);
        }

        #endregion
    }
}

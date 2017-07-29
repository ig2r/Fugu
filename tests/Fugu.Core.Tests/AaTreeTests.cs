using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Fugu.Core.Tests
{
    public class AaTreeTests
    {
        [Fact]
        public void Add_EmptyTree_Succeeds()
        {
            var builder = new AaTree<int>.Builder();
            builder[new byte[] { 0xFF }] = 1;
            var tree = builder.ToImmutable();

            Assert.Equal(new[] { 1 }, tree.ToArray().Select(kvp => kvp.Value));
        }

        [Theory]
        [InlineData(new int[0])]
        [InlineData(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 })]
        [InlineData(new[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 })]
        [InlineData(new[] { 6, 7, 8, 9, 10, 1, 2, 3, 4, 5 })]
        [InlineData(new[] { 1, 10, 2, 9, 3, 8, 4, 7, 5, 6 })]
        [InlineData(new[] { 1, 2, 3, 4, 5, 10, 9, 8, 7, 6 })]
        [InlineData(new[] { 10, 9, 8, 7, 6, 1, 2, 3, 4, 5 })]
        public void Add_VariousKeys_Succeeds(int[] items)
        {
            var builder = new AaTree<int>.Builder();
            foreach (var item in items)
            {
                builder[BitConverter.GetBytes(item)] = item;
            }

            var tree = builder.ToImmutable();
            var expected = items.OrderBy(i => i).ToArray();
            var actual = tree.Select(kvp => kvp.Value).ToArray();
            Assert.Equal(expected, actual);
        }

        [Theory]
        [InlineData(new int[0])]
        [InlineData(new[] { 1 })]
        [InlineData(new[] { 100 })]
        [InlineData(new[] { 50 })]
        [InlineData(new[] { 50, 49, 51 })]
        [InlineData(new[] { 2, 98, 30, 70 })]
        [InlineData(new[] { 100, 99, 98, 97, 96, 95, 94, 93, 92, 91 })]
        [InlineData(new[] { 9, 8, 7, 6, 5, 4, 3, 2, 1 })]
        [InlineData(new[] { 20, 21, 22, 23, 24, 25, 26, 27, 28, 29 })]
        [InlineData(new[] { 25, 26, 27, 28, 29, 20, 21, 22, 23, 24 })]
        public void Remove_VariousKeys_Succeeds(int[] items)
        {
            var range = Enumerable.Range(1, 100).ToArray();

            var builder = new AaTree<int>.Builder();
            foreach (var i in range)
            {
                builder[BitConverter.GetBytes(i)] = i;
            }

            foreach (var item in items)
            {
                builder.Remove(BitConverter.GetBytes(item));
            }

            var tree = builder.ToImmutable();
            var expected = range.Except(items).ToArray();
            var actual = tree.Select(kvp => kvp.Value).ToArray();
            Assert.Equal(expected, actual);
        }
    }
}

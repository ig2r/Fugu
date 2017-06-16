using Fugu.Common;
using System.Runtime.InteropServices;
using Xunit;

namespace Fugu.Core.Tests
{
    public class UnmanagedByteSpanTests
    {
        [Fact]
        public unsafe void Ctor_ZeroLength_CreatesEmptySpan()
        {
            // Arrange
            fixed (byte *p = new byte[1])
            {
                // Act
                var span = new UnmanagedByteSpan(p, 0);

                // Assert
                Assert.Equal(0, span.Length);
            }
        }

        [Fact]
        public unsafe void Indexer_GetIndividualBytes_ReturnsBytes()
        {
            // Arrange
            var data = new byte[] { 0x01, 0x02, 0x03 };
            fixed (byte* p = data)
            {
                var span = new UnmanagedByteSpan(p, data.Length);

                // Act
                var retrieved = new[] { span[0], span[1], span[2] };

                // Assert
                Assert.Equal(data, retrieved);
            }
        }

        [Fact]
        public unsafe void Indexer_SetIndividualBytes_SetsBytes()
        {
            // Arrange
            var destination = new byte[3];
            fixed (byte* p = destination)
            {
                var span = new UnmanagedByteSpan(p, destination.Length);

                // Act
                span[0] = 0x01;
                span[1] = 0x02;
                span[2] = 0x03;
            }

            // Assert
            Assert.Equal(new byte[] { 0x01, 0x02, 0x03 }, destination);
        }

        [Fact]
        public unsafe void Write_SampleStruct_SetsBytes()
        {
            // Arrange
            var destination = new byte[5];

            fixed (byte* p = destination)
            {
                var item = new SampleStruct { Tag = 0x34, Number = 0x12005678 };
                var span = new UnmanagedByteSpan(p, destination.Length);

                // Act
                span.Write(ref item);
            }

            // Assert
            Assert.Equal(new byte[] { 0x34, 0x78, 0x56, 0x00, 0x12 }, destination);
        }

        [Fact]
        public unsafe void Write_EmptySourceArray_Succeeds()
        {
            // Arrange
            var destination = new byte[] { 0x99 };

            fixed (byte* p = destination)
            {
                var span = new UnmanagedByteSpan(p, destination.Length);

                // Act
                span.Write(new byte[0], 0, 0);
            }

            // Assert
            Assert.Equal(new byte[] { 0x99 }, destination);
        }

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        private struct SampleStruct
        {
            public byte Tag;
            public int Number;
        }
    }
}

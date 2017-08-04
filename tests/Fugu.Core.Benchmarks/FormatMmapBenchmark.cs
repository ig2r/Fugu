using BenchmarkDotNet.Attributes;
using System;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fugu.Core.Benchmarks
{
    /// <summary>
    /// Compares approaches to writing structured data to a memory-mapped file.
    /// </summary>
    public class FormatMmapBenchmark
    {
        public const int OPERATIONS = 100000;
        public const int DATALENGTH = 64;

        private MemoryMappedFile _mmap;
        private MemoryMappedViewStream _viewStream;

        private byte[] _buffer = new byte[256];
        private byte[] _data = new byte[DATALENGTH];

        [GlobalSetup]
        public void GlobalSetup()
        {
            long capacity = OPERATIONS * (DATALENGTH + Unsafe.SizeOf<ItemHeader>());
            _mmap = MemoryMappedFile.CreateNew(null, capacity);
            _viewStream = _mmap.CreateViewStream();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _viewStream.Dispose();
            _mmap.Dispose();
        }

        [Benchmark(Baseline = true, OperationsPerInvoke = OPERATIONS)]
        public void UsingViewStream()
        {
            _viewStream.Position = 0;

            for (int i = 0; i < OPERATIONS; i++)
            {
                var itemHeader = new ItemHeader { Tag = 1, KeyLength = 2, ValueLength = 3, Checksum = 0xBEEF };
                var bytes = StructToArray(itemHeader, _buffer);

                _viewStream.Write(bytes.Array, bytes.Offset, bytes.Count);
                _viewStream.Write(_data, 0, _data.Length);
            }
        }

        [Benchmark(OperationsPerInvoke = OPERATIONS)]
        public void UsingSystemSpan()
        {
            Span<byte> span;
            unsafe
            {
                byte* ptr = null;
                _viewStream.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

                checked
                {
                    span = new Span<byte>(ptr, (int)_viewStream.Length);
                }
            }

            for (int i = 0; i < OPERATIONS; i++)
            {
                var itemSpan = span.NonPortableCast<byte, ItemHeader>();
                itemSpan[0] = new ItemHeader { Tag = 1, KeyLength = 2, ValueLength = 3, Checksum = 0xBEEF };
                span = span.Slice(Unsafe.SizeOf<ItemHeader>());

                _data.CopyTo(span);
                span = span.Slice(_data.Length);
            }

            _viewStream.SafeMemoryMappedViewHandle.ReleasePointer();
        }

        private ArraySegment<byte> StructToArray<T>(T structure, byte[] buffer) where T : struct
        {
            var size = Unsafe.SizeOf<ItemHeader>();
            var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            Marshal.StructureToPtr(structure, handle.AddrOfPinnedObject(), false);
            handle.Free();
            return new ArraySegment<byte>(buffer, 0, size);
        }

        #region Nested types

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct ItemHeader
        {
            public byte Tag;
            public int KeyLength;
            public int ValueLength;
            public ulong Checksum;
        }

        #endregion
    }
}

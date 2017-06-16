using BenchmarkDotNet.Attributes;
using Fugu.Common;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Fugu.Core.Benchmarks
{
    public class WriteMmapBenchmark
    {
        private const long CAPACITY = 1024 * 1024;

        private MemoryMappedFile _mmap;
        private MemoryMappedViewStream _viewStream;
        private MemoryMappedViewAccessor _accessor;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _mmap = MemoryMappedFile.CreateNew(null, CAPACITY);
            _viewStream = _mmap.CreateViewStream();
            _accessor = _mmap.CreateViewAccessor();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _accessor.Dispose();
            _viewStream.Dispose();
            _mmap.Dispose();
        }

        [Benchmark]
        public void ViewStream()
        {
            _viewStream.Seek(0, SeekOrigin.Begin);
            for (int i = 0; i < CAPACITY; i++)
            {
                _viewStream.WriteByte(0xFF);
            }
        }

        [Benchmark]
        public void Accessor()
        {
            for (int i = 0; i < CAPACITY; i++)
            {
                _accessor.Write(i, (byte)0xFF);
            }
        }

        [Benchmark(Baseline = true)]
        public void UnsafePointer()
        {
            unsafe
            {
                byte* ptr = null;
                _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

                for (int i = 0; i < CAPACITY; i++)
                {
                    *(ptr + i) = 0xFF;
                }
            }

            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }

        [Benchmark]
        public void UnmanagedByteSpan()
        {
            UnmanagedByteSpan span;

            unsafe
            {
                byte* ptr = null;
                _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                span = new UnmanagedByteSpan(ptr, CAPACITY);
            }

            for (int i = 0; i < CAPACITY; i++)
            {
                span[i] = 0xFF;
            }

            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
}

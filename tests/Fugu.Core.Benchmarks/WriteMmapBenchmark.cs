using BenchmarkDotNet.Attributes;
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

        [IterationSetup]
        public void IterationSetup()
        {
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
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

        [Benchmark]
        public unsafe void UnsafePointer()
        {
            byte* ptr = null;
            _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

            for (int i = 0; i < CAPACITY; i++)
            {
                *ptr = 0xFF;
            }

            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
}

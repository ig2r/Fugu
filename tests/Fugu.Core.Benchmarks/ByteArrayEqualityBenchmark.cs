using BenchmarkDotNet.Attributes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Fugu.Core.Benchmarks
{
    public class ByteArrayEqualityBenchmark
    {
        private readonly List<byte[]> _data = new List<byte[]>();

        [Setup]
        public void Setup()
        {
            const int N = 10;
            var random = new Random();

            _data.Clear();
            for (int i = 0; i < N; i++)
            {
                _data.Add(Encoding.UTF8.GetBytes($"key:{i}"));
            }
        }

        [Benchmark]
        public void UsingStructuralEqualityComparer()
        {
            for (int i = 0; i < _data.Count; i++)
            {
                for (int j = 0; j < _data.Count; j++)
                {
                    var equal = StructuralComparisons.StructuralEqualityComparer.Equals(_data[i], _data[j]);
                }
            }
        }

        [Benchmark]
        public void UsingLinqSequenceEqual()
        {
            for (int i = 0; i < _data.Count; i++)
            {
                for (int j = 0; j < _data.Count; j++)
                {
                    var equal = _data[i].SequenceEqual(_data[j]);
                }
            }
        }

        [Benchmark]
        public void UsingCustomEquals()
        {
            for (int i = 0; i < _data.Count; i++)
            {
                for (int j = 0; j < _data.Count; j++)
                {
                    var equal = ByteArrayEquals(_data[i], _data[j]);
                }
            }
        }

        private static bool ByteArrayEquals(byte[] x, byte[] y)
        {
            if (ReferenceEquals(x, y))
            {
                return true;
            }

            if (x == null || y == null)
            {
                return false;
            }

            if (x.Length != y.Length)
            {
                return false;
            }

            for (int i = 0; i < x.Length; i++)
            {
                if (x[i] != y[i])
                {
                    return false;
                }
            }

            return true;
        }
    }
}

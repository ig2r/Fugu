using BenchmarkDotNet.Attributes;
using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

namespace Fugu.Core.Benchmarks
{
    //[MemoryDiagnoser]
    public class CritBitTreeInsertionBenchmark
    {
        private readonly Random _random = new Random();
        private readonly List<byte[]> _keys = new List<byte[]>();

        public const int ITEM_COUNT = 10000;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _keys.Clear();
            for (int i = 0; i < ITEM_COUNT; i++)
            {
                var key = $"bucket:{_random.Next(256)}/key:{_random.Next(256)}/facet:{_random.Next(256)}";
                _keys.Add(Encoding.UTF8.GetBytes(key));
            }
        }

        [Benchmark(OperationsPerInvoke = ITEM_COUNT)]
        public void InsertIntoCritBitTree()
        {
            var tree = CritBitTree<ByteArrayKeyTraits, byte[], int>.Empty;

            for (int i = 0; i < _keys.Count; i++)
            {
                tree = tree.SetItem(_keys[i], i);
            }
        }

        [Benchmark(OperationsPerInvoke = ITEM_COUNT)]
        public void InsertIntoImmutableDictionary()
        {
            var dict = ImmutableDictionary.Create<byte[], int>(new ByteArrayEqualityComparer());

            for (int i = 0; i < _keys.Count; i++)
            {
                dict = dict.SetItem(_keys[i], i);
            }
        }

        [Benchmark(OperationsPerInvoke = ITEM_COUNT)]
        public void InsertIntoImmutableSortedDictionary()
        {
            var dict = ImmutableSortedDictionary.Create<byte[], int>(new ByteArrayComparer());

            for (int i = 0; i < _keys.Count; i++)
            {
                dict = dict.SetItem(_keys[i], i);
            }
        }

        // Needed for ImmutableDictionary
        private class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
        {
            public bool Equals(byte[] x, byte[] y)
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

            public int GetHashCode(byte[] obj)
            {
                return obj.GetHashCode();
            }
        }

        // Needed for ImmutableSortedDictionary
        private class ByteArrayComparer : IComparer<byte[]>
        {
            public int Compare(byte[] x, byte[] y)
            {
                if (ReferenceEquals(x, y))
                {
                    return 0;
                }

                for (int i = 0; i < x.Length && i < y.Length; i++)
                {
                    if (x[i] != y[i])
                    {
                        return x[i].CompareTo(y[i]);
                    }
                }

                return x.Length.CompareTo(y.Length);
            }
        }
    }
}

using Fugu.Common;
using Fugu.IO.Records;
using System;
using System.Runtime.CompilerServices;

namespace Fugu.IO
{
    /// <summary>
    /// Reads fundamental structural elements from an <see cref="ITable"/> instance.
    /// </summary>
    public sealed class TableReader
    {
        private readonly ITable _table;

        public TableReader(ITable table)
        {
            Guard.NotNull(table, nameof(table));
            _table = table;
        }

        public long Offset { get; private set; }

        public byte GetTag()
        {
            var span = GetSpan();
            return span[0];
        }

        public TableHeaderRecord ReadTableHeader()
        {
            return ReadStruct<TableHeaderRecord>();
        }

        public CommitHeaderRecord ReadCommitHeader()
        {
            return ReadStruct<CommitHeaderRecord>();
        }

        public PutRecord ReadPut()
        {
            return ReadStruct<PutRecord>();
        }

        public TombstoneRecord ReadTombstone()
        {
            return ReadStruct<TombstoneRecord>();
        }

        public ReadOnlySpan<byte> ReadBytes(int count)
        {
            var span = GetSpan().Slice(0, count);
            Offset += count;
            return span;
        }

        public CommitFooterRecord ReadCommitFooter()
        {
            return ReadStruct<CommitFooterRecord>();
        }

        public TableFooterRecord ReadTableFooter()
        {
            return ReadStruct<TableFooterRecord>();
        }

        private T ReadStruct<T>() where T : struct
        {
            var span = GetSpan();
            var structSpan = span.NonPortableCast<byte, T>();
            Offset += Unsafe.SizeOf<T>();
            return structSpan[0];
        }

        private ReadOnlySpan<byte> GetSpan()
        {
            return _table.GetSpan(Offset);
        }
    }
}

using Fugu.Common;
using Fugu.IO.Records;
using System;
using System.Runtime.CompilerServices;

namespace Fugu.IO
{
    /// <summary>
    /// Writes fundamental structural elements to an <see cref="IWritableTable"/> instance.
    /// </summary>
    public sealed class TableWriter
    {
        private readonly IWritableTable _table;

        public TableWriter(IWritableTable table)
        {
            Guard.NotNull(table, nameof(table));
            _table = table;
        }

        public long Offset { get; private set; }

        public void WriteTableHeader(long minGeneration, long maxGeneration)
        {
            var span = GetSpan();
            var structSize = Unsafe.SizeOf<TableHeaderRecord>();

            var records = span.NonPortableCast<byte, TableHeaderRecord>();
            records[0] = new TableHeaderRecord
            {
                Magic = 0xDEADBEEFL,
                FormatVersionMajor = 0,
                FormatVersionMinor = 1,
                MinGeneration = minGeneration,
                MaxGeneration = maxGeneration,
                HeaderChecksum = 0,     // TODO: Calculate for Magic, Major, Minor, generations
            };

            Offset += structSize;
        }

        public void WriteCommitHeader(int count)
        {
            var span = GetSpan();
            var structSize = Unsafe.SizeOf<CommitHeaderRecord>();

            var records = span.NonPortableCast<byte, CommitHeaderRecord>();
            records[0] = new CommitHeaderRecord
            {
                Tag = TableRecordType.CommitHeader,
                Count = count
            };

            Offset += structSize;
        }

        public void WritePut(byte[] key, int valueLength)
        {
            var span = GetSpan();
            var structSize = Unsafe.SizeOf<PutRecord>();

            var records = span.NonPortableCast<byte, PutRecord>();
            records[0] = new PutRecord
            {
                Tag = CommitRecordType.Put,
                KeyLength = (short)key.Length,
                ValueLength = valueLength
            };

            Offset += structSize;
            span = span.Slice(structSize);

            key.CopyTo(span);
            Offset += key.Length;
        }

        public void WriteTombstone(byte[] key)
        {
            var span = GetSpan();
            var structSize = Unsafe.SizeOf<TombstoneRecord>();

            var records = span.NonPortableCast<byte, TombstoneRecord>();
            records[0] = new TombstoneRecord
            {
                Tag = CommitRecordType.Tombstone,
                KeyLength = (short)key.Length
            };

            Offset += structSize;
            span = span.Slice(structSize);

            key.CopyTo(span);
            Offset += key.Length;
        }

        public void WriteCommitFooter(uint commitChecksum)
        {
            var span = GetSpan();
            var structSize = Unsafe.SizeOf<CommitFooterRecord>();

            var records = span.NonPortableCast<byte, CommitFooterRecord>();
            records[0] = new CommitFooterRecord
            {
                CommitChecksum = commitChecksum
            };

            Offset += structSize;
        }

        public void WriteTableFooter(ulong checksum)
        {
            var span = GetSpan();
            var structSize = Unsafe.SizeOf<TableFooterRecord>();

            var records = span.NonPortableCast<byte, TableFooterRecord>();
            records[0] = new TableFooterRecord
            {
                Tag = TableRecordType.TableFooter,
                Checksum = checksum,
            };

            Offset += structSize;
        }

        public void Write(ReadOnlySpan<byte> source)
        {
            var span = GetSpan();
            source.CopyTo(span);
            Offset += source.Length;
        }

        private Span<byte> GetSpan()
        {
            return _table.GetSpan(Offset);
        }
    }
}

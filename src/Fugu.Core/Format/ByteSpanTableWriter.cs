using Fugu.Common;
using System.Runtime.CompilerServices;

namespace Fugu.Format
{
    public class ByteSpanTableWriter<TByteSpan> : TableWriter
        where TByteSpan : struct, IByteSpan<TByteSpan>
    {
        private TByteSpan _span;
        private long _position = 0;

        public ByteSpanTableWriter(TByteSpan span)
        {
            _span = span;
        }

        public override long Position => _position;

        #region TableWriterBase

        protected override void WriteTableHeaderCore(TableHeaderRecord headerRecord)
        {
            _span = _span.Write(ref headerRecord);
            _position += Unsafe.SizeOf<TableHeaderRecord>();
        }

        protected override void WriteTableFooterCore(TableFooterRecord footerRecord)
        {
            _span = _span.Write(ref footerRecord);
            _position += Unsafe.SizeOf<TableFooterRecord>();
        }

        protected override void WriteCommitHeaderCore(CommitHeaderRecord commitHeaderRecord)
        {
            _span = _span.Write(ref commitHeaderRecord);
            _position += Unsafe.SizeOf<CommitHeaderRecord>();
        }

        protected override void WriteCommitFooterCore(CommitFooterRecord commitFooterRecord)
        {
            _span = _span.Write(ref commitFooterRecord);
            _position += Unsafe.SizeOf<CommitFooterRecord>();
        }

        protected override void WritePutCore(PutRecord putRecord, byte[] key)
        {
            _span = _span.Write(ref putRecord);
            _span = _span.Write(key, 0, key.Length);
            _position += Unsafe.SizeOf<PutRecord>() + key.Length;
        }

        protected override void WriteTombstoneCore(TombstoneRecord tombstoneRecord, byte[] key)
        {
            _span = _span.Write(ref tombstoneRecord);
            _span = _span.Write(key, 0, key.Length);
            _position += Unsafe.SizeOf<TombstoneRecord>() + key.Length;
        }

        protected override void WriteArrayCore(byte[] buffer, int sourceIndex, int length)
        {
            _span = _span.Write(buffer, sourceIndex, length);
            _position += length;
        }

        #endregion
    }
}

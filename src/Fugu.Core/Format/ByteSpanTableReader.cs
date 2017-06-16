using Fugu.Common;
using System.Runtime.CompilerServices;

namespace Fugu.Format
{
    public class ByteSpanTableReader<TByteSpan> : TableReader
        where TByteSpan : struct, IByteSpan<TByteSpan>
    {
        private TByteSpan _span;
        private long _position = 0;

        public override long Position => _position;

        public ByteSpanTableReader(TByteSpan span)
        {
            _span = span;
        }

        protected override byte GetTagCore()
        {
            return _span[0];
        }

        protected override TableHeaderRecord ReadTableHeaderCore()
        {
            return ReadStruct<TableHeaderRecord>();
        }

        protected override CommitHeaderRecord ReadCommitHeaderCore()
        {
            return ReadStruct<CommitHeaderRecord>();
        }

        protected override PutRecord ReadPutCore()
        {
            return ReadStruct<PutRecord>();
        }

        protected override TombstoneRecord ReadTombstoneCore()
        {
            return ReadStruct<TombstoneRecord>();
        }

        protected override byte[] ReadBytesCore(int count)
        {
            var bytes = new byte[count];
            _span.Slice(0, count).CopyTo(bytes);
            _span = _span.Slice(count);
            _position += count;
            return bytes;
        }

        protected override CommitFooterRecord ReadCommitFooterCore()
        {
            return ReadStruct<CommitFooterRecord>();
        }

        protected override TableFooterRecord ReadTableFooterCore()
        {
            return ReadStruct<TableFooterRecord>();
        }

        private T ReadStruct<T>() where T : struct
        {
            var item = _span.Read<T>();
            var itemSize = Unsafe.SizeOf<T>();
            _span = _span.Slice(itemSize);
            _position += itemSize;
            return item;
        }
    }
}

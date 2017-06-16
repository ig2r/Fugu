using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Format
{
    public abstract class TableWriter : IDisposable
    {
        public void WriteTableHeader(long minGeneration, long maxGeneration)
        {
            var headerRecord = new TableHeaderRecord
            {
                Magic = 0xDEADBEEFL,
                FormatVersionMajor = 0,
                FormatVersionMinor = 1,
                MinGeneration = minGeneration,
                MaxGeneration = maxGeneration,
                HeaderChecksum = 0,     // TODO: Calculate for Magic, Major, Minor, generations
            };

            WriteTableHeaderCore(headerRecord);
        }

        public abstract long Position { get; }

        public void WriteTableFooter()
        {
            var footerRecord = new TableFooterRecord
            {
                Tag = TableRecordType.TableFooter,
            };

            WriteTableFooterCore(footerRecord);
        }

        public void WriteCommitHeader(int count)
        {
            var commitHeaderRecord = new CommitHeaderRecord
            {
                Tag = TableRecordType.CommitHeader,
                Count = count
            };

            WriteCommitHeaderCore(commitHeaderRecord);
        }

        public void WriteCommitFooter(uint commitChecksum)
        {
            var commitFooterRecord = new CommitFooterRecord
            {
                CommitChecksum = commitChecksum,
            };

            WriteCommitFooterCore(commitFooterRecord);
        }

        public void WritePut(byte[] key, int valueLength)
        {
            Guard.NotNull(key, nameof(key));

            var putRecord = new PutRecord
            {
                Tag = CommitRecordType.Put,
                KeyLength = (short)key.Length,
                ValueLength = valueLength
            };

            WritePutCore(putRecord, key);
        }

        public void WriteTombstone(byte[] key)
        {
            Guard.NotNull(key, nameof(key));

            var tombstoneRecord = new TombstoneRecord
            {
                Tag = CommitRecordType.Tombstone,
                KeyLength = (short)key.Length
            };

            WriteTombstoneCore(tombstoneRecord, key);
        }

        public void Write(byte[] buffer, int sourceIndex, int length)
        {
            WriteArrayCore(buffer, sourceIndex, length);
        }

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion

        protected abstract void WriteTableHeaderCore(TableHeaderRecord headerRecord);
        protected abstract void WriteTableFooterCore(TableFooterRecord footerRecord);
        protected abstract void WriteCommitHeaderCore(CommitHeaderRecord commitHeaderRecord);
        protected abstract void WriteCommitFooterCore(CommitFooterRecord commitFooterRecord);
        protected abstract void WritePutCore(PutRecord putRecord, byte[] key);
        protected abstract void WriteTombstoneCore(TombstoneRecord tombstoneRecord, byte[] key);
        protected abstract void WriteArrayCore(byte[] buffer, int sourceIndex, int length);

        protected virtual void Dispose(bool disposing)
        {
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Format
{
    public abstract class TableReader : IDisposable
    {
        public abstract long Position { get; }

        public byte GetTag()
        {
            return GetTagCore();
        }

        public TableHeaderRecord ReadTableHeader()
        {
            return ReadTableHeaderCore();
        }

        public CommitHeaderRecord ReadCommitHeader()
        {
            return ReadCommitHeaderCore();
        }

        public PutRecord ReadPut()
        {
            return ReadPutCore();
        }

        public TombstoneRecord ReadTombstone()
        {
            return ReadTombstoneCore();
        }

        public byte[] ReadBytes(int count)
        {
            return ReadBytesCore(count);
        }

        public CommitFooterRecord ReadCommitFooter()
        {
            return ReadCommitFooterCore();
        }

        public TableFooterRecord ReadTableFooter()
        {
            return ReadTableFooterCore();
        }

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion

        protected virtual void Dispose(bool disposing)
        {
        }

        protected abstract byte GetTagCore();
        protected abstract TableHeaderRecord ReadTableHeaderCore();
        protected abstract CommitHeaderRecord ReadCommitHeaderCore();
        protected abstract PutRecord ReadPutCore();
        protected abstract TombstoneRecord ReadTombstoneCore();
        protected abstract byte[] ReadBytesCore(int count);
        protected abstract CommitFooterRecord ReadCommitFooterCore();
        protected abstract TableFooterRecord ReadTableFooterCore();
    }
}

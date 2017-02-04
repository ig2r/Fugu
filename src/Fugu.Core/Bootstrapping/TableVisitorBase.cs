using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Fugu.Format;

namespace Fugu.Bootstrapping
{
    public abstract class TableVisitorBase : ITableVisitor
    {
        #region ITableVisitor

        public virtual void OnTableHeader(TableHeaderRecord header)
        {
        }

        public virtual void OnCommit(IEnumerable<byte[]> tombstones, IEnumerable<ParsedPutRecord> puts, ulong commitChecksum)
        {
        }

        public virtual void OnTableFooter()
        {
        }

        public virtual void OnError(Exception exception)
        {
        }

        #endregion
    }
}

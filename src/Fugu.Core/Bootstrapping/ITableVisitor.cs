using Fugu.Format;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public interface ITableVisitor
    {
        void OnTableHeader(TableHeaderRecord header);
        void OnCommit(IEnumerable<byte[]> tombstones, IEnumerable<ParsedPutRecord> puts, ulong commitChecksum);
        void OnTableFooter();

        void OnError(Exception exception);
    }
}

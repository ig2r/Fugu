using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Bootstrapping
{
    public enum SegmentParserTokenType
    {
        NotStarted = 0,
        Faulted,

        TableHeader,
        TableFooter,
        CommitHeader,
        CommitFooter,
        Put,
        Tombstone,
        Value,
    }
}

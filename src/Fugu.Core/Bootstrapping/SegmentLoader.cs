using Fugu.Actors;
using Fugu.Common;
using Fugu.Index;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Bootstrapping
{
    public class SegmentLoader : ISegmentLoader
    {
        private readonly TableParser _parser;
        private readonly ITargetBlock<UpdateIndexMessage> _indexUpdateBlock;

        public SegmentLoader(TableParser parser, ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
        {
            Guard.NotNull(parser, nameof(parser));
            Guard.NotNull(indexUpdateBlock, nameof(indexUpdateBlock));

            _parser = parser;
            _indexUpdateBlock = indexUpdateBlock;
        }

        #region ISegmentLoader

        public async Task<bool> CheckTableFooterAsync(ITable table)
        {
            Guard.NotNull(table, nameof(table));

            var visitor = new FooterCheckVisitor();
            await _parser.ParseAsync(table, visitor);
            return visitor.HasValidFooter.Value;
        }

        public Task LoadSegmentAsync(Segment segment, bool verifyChecksums)
        {
            Guard.NotNull(segment, nameof(segment));

            // TODO: use different parsing strategy based on whether checksums need to be verified
            var visitor = new FastForwardLoadVisitor(segment, _indexUpdateBlock);
            var parseTask = _parser.ParseAsync(segment.Table, visitor);
            return Task.WhenAll(parseTask, visitor.Completion);
        }

        #endregion
    }
}

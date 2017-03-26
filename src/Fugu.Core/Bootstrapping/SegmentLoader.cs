using Fugu.Actors;
using Fugu.Channels;
using Fugu.Common;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public class SegmentLoader : ISegmentLoader
    {
        private readonly TableParser _parser;
        private readonly IIndexActor _indexActor;
        private readonly Channel<UpdateIndexMessage> _indexUpdateChannel;

        public SegmentLoader(TableParser parser, IIndexActor indexActor, Channel<UpdateIndexMessage> indexUpdateChannel)
        {
            Guard.NotNull(parser, nameof(parser));
            Guard.NotNull(indexActor, nameof(indexActor));
            Guard.NotNull(indexUpdateChannel, nameof(indexUpdateChannel));

            _parser = parser;
            _indexActor = indexActor;
            _indexUpdateChannel = indexUpdateChannel;
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
            var visitor = new FastForwardLoadVisitor(segment, _indexUpdateChannel);
            var parseTask = _parser.ParseAsync(segment.Table, visitor);
            return Task.WhenAll(parseTask, visitor.Completion);
        }

        #endregion
    }
}

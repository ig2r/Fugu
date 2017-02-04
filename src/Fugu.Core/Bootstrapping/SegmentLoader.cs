using Fugu.Actors;
using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public class SegmentLoader : ISegmentLoader
    {
        private readonly TableParser _parser;
        private readonly IIndexActor _indexActor;

        /// <summary>
        /// Initializes a new instance of the <see cref="SegmentLoader"/> class.
        /// </summary>
        /// <param name="indexActor">An actor managing the index to populate.</param>
        public SegmentLoader(TableParser parser, IIndexActor indexActor)
        {
            Guard.NotNull(parser, nameof(parser));
            Guard.NotNull(indexActor, nameof(indexActor));

            _parser = parser;
            _indexActor = indexActor;
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
            var visitor = new FastForwardLoadVisitor(segment, _indexActor);
            var parseTask = _parser.ParseAsync(segment.Table, visitor);
            return Task.WhenAll(parseTask, visitor.Completion);
        }

        #endregion
    }
}

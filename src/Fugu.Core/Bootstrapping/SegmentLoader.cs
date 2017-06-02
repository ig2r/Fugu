using Fugu.Actors;
using Fugu.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Bootstrapping
{
    public class SegmentLoader : ISegmentLoader
    {
        private readonly ITargetBlock<UpdateIndexMessage> _indexUpdateBlock;
        private readonly List<SegmentLoadResult> _loadedSegments = new List<SegmentLoadResult>();

        public SegmentLoader(ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
        {
            Guard.NotNull(indexUpdateBlock, nameof(indexUpdateBlock));
            _indexUpdateBlock = indexUpdateBlock;
        }

        public IReadOnlyList<SegmentLoadResult> LoadedSegments => _loadedSegments;

        #region ISegmentLoader

        public async Task<bool> TryLoadSegmentAsync(Segment segment, bool requireValidFooter)
        {
            Guard.NotNull(segment, nameof(segment));

            // First pass over segment to find out if the table holds a valid table footer
            var hasValidFooter = await TryFindTableFooterAsync(segment);
            if (!hasValidFooter && requireValidFooter)
            {
                return false;
            }

            // Second pass over segment to load data into the index. If no valid footer was found during the first
            // pass, we also need to verify integrity via checksums.
            using (var stream = segment.Table.GetInputStream(0, segment.Table.Capacity))
            using (var parser = new SegmentParser(stream))
            {
                long lastGoodPosition = 0;

                try
                {
                    while (await parser.ReadAsync())
                    {
                        // Scan for commit headers, then parse commits into collection of index updates
                        if (parser.Current == SegmentParserTokenType.CommitHeader)
                        {
                            var indexUpdates = await ParseCommitAsync(segment, stream, parser);
                            var replyChannel = new TaskCompletionSource<VoidTaskResult>();
                            await Task.WhenAll(
                                _indexUpdateBlock.SendAsync(new UpdateIndexMessage(new StateVector(), indexUpdates, replyChannel)),
                                replyChannel.Task);
                        }

                        // Remember this position because we may need to resume writing from here if it turns out that
                        // the remainder of the file is corrupt
                        lastGoodPosition = stream.Position;
                    }
                }
                catch { }

                _loadedSegments.Add(new SegmentLoadResult(segment, hasValidFooter, lastGoodPosition));
                return true;
            }
        }

        #endregion

        private async Task<bool> TryFindTableFooterAsync(Segment segment)
        {
            using (var stream = segment.Table.GetInputStream(0, segment.Table.Capacity))
            using (var parser = new SegmentParser(stream))
            {
                try
                {
                    while (await parser.ReadAsync())
                    {
                        switch (parser.Current)
                        {
                            case SegmentParserTokenType.TableFooter:
                                // TODO: verify checksum
                                await parser.ReadTableFooterAsync();
                                return true;
                        }
                    }

                    return false;
                }
                catch
                {
                    return false;
                }
            }
        }

        private async Task<IReadOnlyList<KeyValuePair<byte[], IndexEntry>>> ParseCommitAsync(
            Segment segment,
            Stream stream,
            SegmentParser parser)
        {
            var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>();
            var putKeys = new Queue<byte[]>();

            while (await parser.ReadAsync())
            {
                switch (parser.Current)
                {
                    case SegmentParserTokenType.Put:
                        {
                            var key = await parser.ReadPutOrTombstoneKeyAsync();
                            putKeys.Enqueue(key);
                            break;
                        }
                    case SegmentParserTokenType.Tombstone:
                        {
                            var key = await parser.ReadPutOrTombstoneKeyAsync();
                            indexUpdates.Add(
                                new KeyValuePair<byte[], IndexEntry>(
                                    key,
                                    new IndexEntry.Tombstone(segment)));
                            break;
                        }
                    case SegmentParserTokenType.Value:
                        {
                            var key = putKeys.Dequeue();
                            var offset = stream.Position;
                            var value = await parser.ReadValueAsync();
                            indexUpdates.Add(
                                new KeyValuePair<byte[], IndexEntry>(
                                    key,
                                    new IndexEntry.Value(segment, offset, value.Length)));
                            break;
                        }
                    case SegmentParserTokenType.CommitFooter:
                        {
                            await parser.ReadCommitFooterAsync();
                            return indexUpdates;
                        }
                    default:
                        throw new InvalidOperationException("Unexpected token while parsing commit.");
                }
            }

            throw new InvalidOperationException("Unexpected end of commit.");
        }
    }
}

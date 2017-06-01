using Fugu.Actors;
using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Bootstrapping
{
    public class SegmentLoader : ISegmentLoader
    {
        private readonly ITargetBlock<UpdateIndexMessage> _indexUpdateBlock;
        private readonly List<Segment> _loadedSegments = new List<Segment>();

        public SegmentLoader(ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
        {
            Guard.NotNull(indexUpdateBlock, nameof(indexUpdateBlock));
            _indexUpdateBlock = indexUpdateBlock;
        }

        public IReadOnlyList<Segment> LoadedSegments => _loadedSegments;

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
                try
                {
                    while (await parser.ReadAsync())
                    {
                        // Scan for commit headers
                        if (parser.Current == SegmentParserTokenType.CommitHeader)
                        {
                            var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>();
                            var putKeys = new Queue<byte[]>();

                            while (await parser.ReadAsync())
                            {
                                if (parser.Current == SegmentParserTokenType.CommitFooter)
                                {
                                    var replyChannel = new TaskCompletionSource<VoidTaskResult>();
                                    await Task.WhenAll(
                                        _indexUpdateBlock.SendAsync(
                                            new UpdateIndexMessage(new StateVector(), indexUpdates, replyChannel)),
                                        replyChannel.Task);
                                    break;
                                }

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
                                    default:
                                        throw new InvalidOperationException("Unexpected token while parsing commit.");
                                }
                            }
                        }
                    }
                }
                catch { }

                _loadedSegments.Add(segment);
                return true;
            }
        }

        #endregion

        public async Task<bool> TryFindTableFooterAsync(Segment segment)
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
                                var tableHeader = await parser.ReadTableFooterAsync();
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
    }
}

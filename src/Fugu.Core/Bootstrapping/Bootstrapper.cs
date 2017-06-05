using Fugu.Actors;
using Fugu.Common;
using Fugu.Format;
using Fugu.Index;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Bootstrapping
{
    /// <summary>
    /// Populates a <see cref="KeyValueStore"/> instance with data from a given table set by parsing the
    /// tables contained therein and replaying the respective change events.
    /// </summary>
    public class Bootstrapper
    {
        public async Task<BootstrapperResult> RunAsync(
            ITableSet tableSet,
            ITargetBlock<UpdateIndexMessage> indexUpdateBlock,
            ITargetBlock<Segment> segmentCreatedBlock)
        {
            Guard.NotNull(tableSet, nameof(tableSet));
            Guard.NotNull(indexUpdateBlock, nameof(indexUpdateBlock));
            Guard.NotNull(segmentCreatedBlock, nameof(segmentCreatedBlock));

            // Enumerate available segments
            var tables = await tableSet.GetTablesAsync();
            var availableSegments = await GetAvailableSegmentsAsync(tables);

            // Scan segments to populate index
            var loadStrategy = new SegmentLoadStrategy(availableSegments);
            var loadedSegments = new List<Segment>();
            long maxGeneration = 0;

            while (loadStrategy.GetNext(maxGeneration, out var segment, out var requireValidFooter))
            {
                if (await TryLoadSegmentAsync(segment, requireValidFooter, indexUpdateBlock))
                {
                    loadedSegments.Add(segment);
                    maxGeneration = segment.MaxGeneration;

                    // Notify observers that a new segment is available
                    segmentCreatedBlock.Post(segment);
                }
            }

            var totalCapacity = loadedSegments.Sum(s => s.Table.Capacity);
            return new BootstrapperResult(maxGeneration, totalCapacity);
        }

        private async Task<IEnumerable<Segment>> GetAvailableSegmentsAsync(IEnumerable<ITable> tables)
        {                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
            var tasks = new HashSet<Task<Segment>>(tables.Select(ReadTableHeaderAsync));
            var segments = new List<Segment>();

            while (tasks.Count > 0)
            {
                var task = await Task.WhenAny(tasks);
                tasks.Remove(task);

                if (task.IsCompleted)
                {
                    segments.Add(task.Result);
                }
            }

            return segments;
        }

        private async Task<Segment> ReadTableHeaderAsync(ITable table)
        {
            using (var input = table.GetInputStream(0, table.Capacity))
            using (var reader = new TableReader(input))
            {
                var header = await reader.ReadTableHeaderAsync();
                return new Segment(header.MinGeneration, header.MaxGeneration, table);
            }
        }

        private async Task<bool> TryLoadSegmentAsync(
            Segment segment,
            bool requireValidFooter,
            ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
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
                        // Scan for commit headers, then parse commits into collection of index updates
                        if (parser.Current == SegmentParserTokenType.CommitHeader)
                        {
                            var indexUpdates = await ParseCommitAsync(segment, stream, parser);
                            var replyChannel = new TaskCompletionSource<VoidTaskResult>();
                            await Task.WhenAll(
                                indexUpdateBlock.SendAsync(new UpdateIndexMessage(new StateVector(), indexUpdates, replyChannel)),
                                replyChannel.Task);
                        }
                    }
                }
                catch { }
                return true;
            }
        }

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

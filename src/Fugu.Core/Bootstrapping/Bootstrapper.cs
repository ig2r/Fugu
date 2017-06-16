using Fugu.Actors;
using Fugu.Common;
using Fugu.Format;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
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

            // Discard tables we didn't load
            var unusedTables = tables.Except(loadedSegments.Select(s => s.Table));
            await Task.WhenAll(unusedTables.Select(tableSet.RemoveTableAsync));

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

        private Task<Segment> ReadTableHeaderAsync(ITable table)
        {
            try
            {
                using (var reader = table.GetReader(0, table.Capacity))
                {
                    var header = reader.ReadTableHeader();
                    var segment = new Segment(header.MinGeneration, header.MaxGeneration, table);
                    return Task.FromResult(segment);
                }
            }
            catch (Exception ex)
            {
                return Task.FromException<Segment>(ex);
            }
        }

        private async Task<bool> TryLoadSegmentAsync(
            Segment segment,
            bool requireValidFooter,
            ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
        {
            Guard.NotNull(segment, nameof(segment));

            // First pass over segment to find out if the table holds a valid table footer
            var hasValidFooter = TryFindTableFooter(segment);
            if (!hasValidFooter && requireValidFooter)
            {
                return false;
            }

            // Second pass over segment to load data into the index. If no valid footer was found during the first
            // pass, we also need to verify integrity via checksums.
            using (var reader = segment.Table.GetReader(0, segment.Table.Capacity))
            {
                var parser = new SegmentParser(reader);
                    
                try
                {
                    while (parser.Read())
                    {
                        // Scan for commit headers, then parse commits into collection of index updates
                        if (parser.Current == SegmentParserTokenType.CommitHeader)
                        {
                            var indexUpdates = ParseCommit(segment, reader, parser, verifyChecksum: !hasValidFooter);
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

        private bool TryFindTableFooter(Segment segment)
        {
            using (var reader = segment.Table.GetReader(0, segment.Table.Capacity))
            {
                var parser = new SegmentParser(reader);

                try
                {
                    while (parser.Read())
                    {
                        switch (parser.Current)
                        {
                            case SegmentParserTokenType.TableFooter:
                                // TODO: verify checksum
                                parser.ReadTableFooter();
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

        private IReadOnlyList<KeyValuePair<byte[], IndexEntry>> ParseCommit(
            Segment segment,
            TableReader reader,
            SegmentParser parser,
            bool verifyChecksum)
        {
            var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>();
            var putKeys = new Queue<byte[]>();

            var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);

            while (parser.Read())
            {
                switch (parser.Current)
                {
                    case SegmentParserTokenType.Put:
                        {
                            var key = parser.ReadPutOrTombstoneKey();
                            putKeys.Enqueue(key);

                            if (verifyChecksum)
                            {
                                md5.AppendData(key);
                            }

                            break;
                        }
                    case SegmentParserTokenType.Tombstone:
                        {
                            var key = parser.ReadPutOrTombstoneKey();
                            indexUpdates.Add(
                                new KeyValuePair<byte[], IndexEntry>(
                                    key,
                                    new IndexEntry.Tombstone(segment)));

                            if (verifyChecksum)
                            {
                                md5.AppendData(key);
                            }

                            break;
                        }
                    case SegmentParserTokenType.Value:
                        {
                            var key = putKeys.Dequeue();
                            var offset = reader.Position;
                            var value = parser.ReadValue();
                            indexUpdates.Add(
                                new KeyValuePair<byte[], IndexEntry>(
                                    key,
                                    new IndexEntry.Value(segment, offset, value.Length)));

                            if (verifyChecksum)
                            {
                                md5.AppendData(value);
                            }

                            break;
                        }
                    case SegmentParserTokenType.CommitFooter:
                        {
                            var commitFooter = parser.ReadCommitFooter();

                            if (verifyChecksum)
                            {
                                var hash = md5.GetHashAndReset();
                                uint checksum = hash[0] | (uint)(hash[1] << 8) | (uint)(hash[2] << 16) | (uint)(hash[3] << 24);

                                if (checksum != commitFooter.CommitChecksum)
                                {
                                    throw new InvalidDataException("Mismatched checksum, this indicates possible data corruption.");
                                }
                            }

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

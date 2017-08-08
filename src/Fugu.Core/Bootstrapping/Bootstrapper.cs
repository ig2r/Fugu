using Fugu.Actors;
using Fugu.Common;
using Fugu.IO;
using Fugu.IO.Records;
using System;
using System.Collections.Generic;
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
            var availableSegments = GetAvailableSegments(tables);

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

        private IEnumerable<Segment> GetAvailableSegments(IEnumerable<ITable> tables)
        {
            var segments = new List<Segment>();

            foreach (var table in tables)
            {
                try
                {
                    var reader = new TableReader(table);
                    var header = reader.ReadTableHeader();
                    var segment = new Segment(header.MinGeneration, header.MaxGeneration, table);
                    segments.Add(segment);
                }
                catch
                {
                    // We'll delete this table later
                }
            }

            return segments;
        }

        private async Task<bool> TryLoadSegmentAsync(
            Segment segment,
            bool requireValidFooter,
            ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
        {
            Guard.NotNull(segment, nameof(segment));

            // Read structure
            var tableReader = new TableReader(segment.Table);
            var header = tableReader.ReadTableHeader();

            var commitReader = new CommitReader(segment, tableReader);
            var commitInfos = new List<CommitInfo>();

            while ((TableRecordType)tableReader.GetTag() == TableRecordType.CommitHeader)
            {
                var commitInfo = commitReader.ReadCommit();
                commitInfos.Add(commitInfo);
            }

            var tableFooter = tableReader.ReadTableFooter();

            var expectedChecksum = (ulong)header.MinGeneration ^ (ulong)header.MaxGeneration;
            var validFooter = tableFooter.Tag == TableRecordType.TableFooter &&
                tableFooter.Checksum == expectedChecksum;

            if (requireValidFooter && !validFooter)
            {
                return false;
            }

            // TODO: check if footer checksum matches header; in that case, we could skip verifying individual commits

            // Verify commit checksums
            var goodCommitCount = VerifyCommitChecksums(commitInfos);
            if (goodCommitCount < commitInfos.Count && requireValidFooter)
            {
                return false;
            }

            var goodCommitInfos = commitInfos.GetRange(0, goodCommitCount);
            await AddCommitsToIndexAsync(goodCommitInfos, indexUpdateBlock);

            return true;
        }

        private int VerifyCommitChecksums(IReadOnlyList<CommitInfo> commitInfos)
        {
            var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
            int goodCommitCount = 0;

            foreach (var commitInfo in commitInfos)
            {
                // Include keys
                foreach (var indexUpdate in commitInfo.IndexUpdates)
                {
                    md5.AppendData(indexUpdate.Key);
                }

                // Include values
                foreach (var indexUpdate in commitInfo.IndexUpdates)
                {
                    if (indexUpdate.Value is IndexEntry.Value put)
                    {
                        var value = put.Segment.Table.GetSpan(put.Offset).Slice(0, put.ValueLength);
                        md5.AppendData(value.ToArray());
                    }
                }

                var hash = md5.GetHashAndReset();
                uint checksum = hash[0] | (uint)(hash[1] << 8) | (uint)(hash[2] << 16) | (uint)(hash[3] << 24);

                if (checksum != commitInfo.CommitChecksum)
                {
                    break;
                }

                goodCommitCount++;
            }

            return goodCommitCount;
        }

        private Task AddCommitsToIndexAsync(IReadOnlyList<CommitInfo> commitInfos, ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
        {
            var sendTasks = new List<Task>(commitInfos.Count + 1);
            TaskCompletionSource<VoidTaskResult> replyChannel = null;

            for (int i = 0; i < commitInfos.Count; i++)
            {
                var commitInfo = commitInfos[i];

                if (i == commitInfos.Count - 1)
                {
                    replyChannel = new TaskCompletionSource<VoidTaskResult>();
                }

                sendTasks.Add(indexUpdateBlock.SendAsync(
                    new UpdateIndexMessage(
                        new StateVector(), commitInfo.IndexUpdates, replyChannel)));

                if (replyChannel != null)
                {
                    sendTasks.Add(replyChannel.Task);
                }
            }

            return Task.WhenAll(sendTasks);
        }
    }
}

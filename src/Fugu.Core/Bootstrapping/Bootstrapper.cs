using Fugu.Actors;
using Fugu.Common;
using Fugu.Format;
using Fugu.Index;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Bootstrapping
{
    /// <summary>
    /// Handles initialization of a new <see cref="KeyValueStore"/> instance.
    /// </summary>
    public class Bootstrapper
    {
        public async Task<BootstrapperResult> RunAsync(ITableSet tableSet, ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
        {
            Guard.NotNull(tableSet, nameof(tableSet));
            Guard.NotNull(indexUpdateBlock, nameof(indexUpdateBlock));

            // Enumerate available segments
            var tables = await tableSet.GetTablesAsync();
            var availableSegments = await GetAvailableSegmentsAsync(tables);

            // Scan segments to populate index
            var segmentLoader = new SegmentLoader(indexUpdateBlock);
            var loadStrategy = new SegmentLoadStrategy();
            await loadStrategy.RunAsync(availableSegments, segmentLoader);

            var maxGenerationLoaded = segmentLoader.LoadedSegments.Any()
                ? segmentLoader.LoadedSegments.Max(s => s.MaxGeneration)
                : 0;

            return new BootstrapperResult(maxGenerationLoaded, segmentLoader.LoadedSegments);
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
    }
}

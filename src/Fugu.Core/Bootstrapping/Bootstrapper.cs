using Fugu.Actors;
using Fugu.Common;
using Fugu.Format;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    /// <summary>
    /// Handles initialization of a new <see cref="KeyValueStore"/> instance.
    /// </summary>
    public class Bootstrapper
    {
        public async Task<BootstrapperResult> RunAsync(ITableSet tableSet, IIndexActor indexActor)
        {
            Guard.NotNull(tableSet, nameof(tableSet));
            Guard.NotNull(indexActor, nameof(indexActor));

            // Walk through tables in table set and try to load segment metadata
            var tables = await tableSet.GetTablesAsync();
            var availableSegments = await GetAvailableSegmentsAsync(tables);

            var loadStrategy = new SegmentLoadStrategy();
            var tableLoader = new SegmentLoader(new TableParser(), indexActor);

            var loadedSegments = await loadStrategy.RunAsync(availableSegments, tableLoader);

            var maxGenerationLoaded = loadedSegments.Any()
                ? loadedSegments.Max(s => s.MaxGeneration)
                : 0;

            return new BootstrapperResult(maxGenerationLoaded);
        }

        private async Task<IEnumerable<Segment>> GetAvailableSegmentsAsync(IEnumerable<ITable> tables)
        {
            var availableSegments = new List<Segment>();
            var corruptTables = new List<ITable>();

            var headerRetrievals = tables.ToDictionary(t => ReadTableHeaderAsync(t));
            while (headerRetrievals.Count > 0)
            {
                var task = await Task.WhenAny(headerRetrievals.Keys);

                if (task.IsCompleted)
                {
                    availableSegments.Add(await task);
                }
                else
                {
                    corruptTables.Add(headerRetrievals[task]);
                }

                headerRetrievals.Remove(task);
            }

            return availableSegments;
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

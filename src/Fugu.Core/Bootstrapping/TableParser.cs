using Fugu.Format;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public class TableParser
    {
        public async Task ParseAsync(ITable table, ITableVisitor visitor)
        {
            using (var input = table.GetInputStream(0, table.Capacity))
            using (var reader = new TableReader(input))
            {
                try
                {
                    var header = await reader.ReadTableHeaderAsync();
                    visitor.OnTableHeader(header);

                    while (true)
                    {
                        var tag = (TableRecordType)reader.ReadTag();
                        switch (tag)
                        {
                            case TableRecordType.CommitHeader:
                                await ParseCommitAsync(reader, visitor);
                                break;
                            case TableRecordType.TableFooter:
                                await reader.ReadTableFooterAsync();
                                visitor.OnTableFooter();
                                return;
                            default:
                                throw new InvalidOperationException("Invalid tag.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    visitor.OnError(ex);
                }
            }
        }

        private async Task ParseCommitAsync(TableReader reader, ITableVisitor visitor)
        {
            // Read header
            var commitHeader = await reader.ReadCommitHeaderAsync();

            // Read put/tombstone records
            var tombstones = new List<byte[]>();
            var putRecords = new List<KeyValuePair<byte[], PutRecord>>();

            for (int i = 0; i < commitHeader.Count; i++)
            {
                var tag = (CommitRecordType)reader.ReadTag();
                switch (tag)
                {
                    case CommitRecordType.Put:
                        {
                            var put = await reader.ReadPutAsync();
                            var key = await reader.ReadBytesAsync(put.KeyLength);
                            putRecords.Add(new KeyValuePair<byte[], PutRecord>(key, put));
                            break;
                        }
                    case CommitRecordType.Tombstone:
                        {
                            var tombstone = await reader.ReadTombstoneAsync();
                            var key = await reader.ReadBytesAsync(tombstone.KeyLength);
                            tombstones.Add(key);
                            break;
                        }
                    default:
                        throw new InvalidOperationException("Invalid tag.");
                }
            }

            // Determine position of put values
            var puts = new List<ParsedPutRecord>();
            foreach (var kvp in putRecords)
            {
                puts.Add(new ParsedPutRecord(kvp.Key, reader.Position, kvp.Value.ValueLength));
                reader.SkipBytes(kvp.Value.ValueLength);
            }

            // Read footer
            var footer = await reader.ReadCommitFooterAsync();

            visitor.OnCommit(tombstones, puts, footer.CommitChecksum);
        }
    }
}

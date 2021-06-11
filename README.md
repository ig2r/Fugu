## Decision log

- Vector clock to have two components: write count and compaction count
- Embed segment deletion with compaction actor. Either a compaction runs, *or* a dead segment is deleted. This ensures that all source segments for a compaction run remain available until the operation completes.
- Each actor has exactly one input channel. For actors that need to process different kinds of messages (e.g., snapshots actor), we use a discriminated union.
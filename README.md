## Decision log

- Vector clock to have two components: write count and compaction count
- Embed segment deletion with compaction actor. Either a compaction runs, *or* a dead segment is deleted. This ensures that all source segments for a compaction run remain available until the operation completes.
- Some actors need to have multiple input channels. Merging messages into a single channel (carrying a discrimnated union type, for example) may not be feasible due to different retention and/or backpressure requirements.

## Actors

### Allocation
### Writer
### Index
### Snapshots
### Segment Stats
### Compaction

## Channels

### allocateWriteBatch
- from: Store
- to: Allocation actor

### writeWriteBatch
- from: Allocation actor
- to: Writer actor

### updateIndex
- from: Writer actor
- from: Compaction actor
- to: Index actor

### indexUpdated
- from: Index actor
- to: Snapshots actor

### acquireSnapshot
- from: Store
- to: Snapshots actor

### releaseSnapshot
- from: Store
- to: Snapshots actor

### updateSegmentStats
- from: Index actor
- to: Segment Stats actor

### segmentStatsUpdated
- from: Segment Stats actor
- to: Compaction actor

### segmentEmptied
- from: Segment Stats actor
- to: Compaction actor

### segmentEvicted
- from: Compaction actor
- to: Allocation actor

### snapshotsUpdated
- from: Snapshots actor
- to: Compaction actor
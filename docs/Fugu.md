# Fugu

*An embeddable, lightweight key-value storage engine for .NET Core.*

## Design

### Objectives

- Simple, maintainable architecture and implementation
- Run on .NET Core and .NET Framework including Linux, macOS, mobile
- Keys and values are byte strings
- Atomic multi-key commits
- Concurrency: MVCC with snapshot isolation

### Non-objectives

- Query language
- Inter-process and/or network API
- Distributed deployment scenarios

### Decisions

- Log-structured, append-only storage scheme for high throughput and robust operation
- Dataflow architecture with actors to contain mutable state; split into reactive *shell* and functional *core*
- Keys held in immutable, in-memory trie index; values accessed through memory map
- No explicit caching, rely on operating system to cache accessed data

## Glossary

- `TableSet`: a collection of tables that together make up an ordered log representing the stored data
- `Table`: a *sorted string table* that holds data of a segment of the conceptual log structure
- `Segment`: a continguous range of modifications to the store, represented as a single table
- `Commit`: an atomic set of modifications applied to the store
- `WriteBatch`: a set of pending changes to make to the store in one atomic commit
- `Snapshot`: an immutable point-in-time snapshot to access data

## Storage Format

    Table ::= TableHeader Commit* TableFooter?

    TableHeader ::=
        Magic                   : uint64
        Format version (major)  : uint16
        Format version (minor)  : uint16
        Min generation          : int64
        Max generation          : int64
        Header checksum         : uint32 - verifies integrity of table header

    Commit ::= CommitHeader {Put, Tombstone}* PutValue* CommitFooter

    CommitHeader ::=
        Tag                     : uint8
        # items in commit       : int32

    Put ::=
        Tag                     : uint8
        Key length in bytes     : int16
        Value length in bytes   : int32
        Key data (bytes)        : variable

    Tombstone ::=
        Tag                     : uint8
        Key length in bytes     : int16
        Key data (bytes)        : variable

    PutValue ::=
        Value data (bytes)      : variable

    CommitFooter ::=
        Commit checksum         : uint32 - verifies integrity of key and value data

    TableFooter ::=
        Tag                     : uint8
        Checksum                : uint64 - verifies integrity of table contents

### Notes

- Tags distinguish between `CommitHeader` and `TableFooter` elements, and between `Put` and `Tombstone` elements within a commit.
- Commit footers holding a checksum of committed data appear at the end of the commit to enable forward-only, streaming writes and checksumming.
- Presence of a table footer indicates that writing of the segment concluded in a controlled manner, i.e., all data committed was flushed to storage before the footer was written. Therefore, loading code can assume that commits are correctly written and may skip checksum verification.

## Bootstrapping

1. From the given `TableSet`, retrieve all available `Table` instances.
2. From each `Table`, read table headers.
3. Sort `Table`s by min generation (ascending), then by max generation (descending).
4. For each `Table` in that ordering:
    1. Check if a table footer is present, and if no footer can be found but following segments cover the samee min generation, skip the current segment.
    2. Otherwise, read contents from current segment, verifying checksums if the current segment had no valid footer.
	3. Skip all further segments that overlap with the read generation range.
5. Truncate all loaded tables to shed extra space beyond the end of the table.

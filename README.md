# Fugu

Fugu is a lightweight, embedded key-value storage engine written in C#. It aims to provide a fast, low-overhead storage option for services and applications on .NET Core and the full .NET Framework. At its core, Fugu is about achieving maximum effect with minimal complexity.

## Status

First and foremost, this is a personal side project of mine that isn't feature-complete or mature enough for production use yet. Nevertheless, I figured that development might as well happen in the open from the beginning.

## Features

- Log-structured, append-only storage scheme similar to the Bitcask backend for Riak.
- MVCC concurrency model with snapshot isolation and atomic multi-key commits.
- Keys and values are plain `byte[]` strings, up to 32 KB / 2 GB in length and in lexicographic order.
- Background compaction to reclaim unused space without stalling writes or reads and with good write amplification.
- Supports on-disk and in-memory (ephemeral) storage, very helpful for testing.

## Usage Example

    byte[] key = ...;
    byte[] data = ...;

    using (var store = await KeyValueStore.CreateAsync(new InMemoryTableSet())
    {
        // Write
        var batch = new WriteBatch();
        batch.Put(key, data);
        await store.CommitAsync(batch);

        // Read
        using (var snapshot = await store.GetSnapshotAsync())
        {
            var retrieved = await snapshot.TryGetValueAsync(key);
        }
    }

## Target Platforms

- .NET Core (.NET Standard Platform 1.3+ for now)
- .NET Framework 4.6+

## Design Decisions

- The store's internal structure reflects the data flow during reads and writes by way of decomposition into a mesh of actors. This affords a clean separation of concerns, curbs global state (and the associated need for global locks), and makes it easier to reason about the behavior of individual components and the store as a whole.
- The master index exists purely in memory, i.e., your keys must fit in RAM and the index is not backed by an on-disk structure. This greatly reduces complexity and guarantees single-seek lookups.
- Snapshot isolation is achieved through the use of an immutable crit-bit tree for the master index.
- For I/O, Fugu favors the OS page cache over a custom caching solution. Data files are mapped into RAM for fast concurrent access.
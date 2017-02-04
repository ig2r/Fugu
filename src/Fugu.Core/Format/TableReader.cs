using Fugu.Common;
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Fugu.Format
{
    /// <summary>
    /// Tokenizes a table data stream.
    /// </summary>
    public class TableReader : IDisposable
    {
        private readonly Stream _input;
        private readonly byte[] _buffer = new byte[256];

        private bool _isDisposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableReader"/> class.
        /// </summary>
        /// <param name="input">Source stream holding elements to read. The current instance takes ownership
        /// of this stream, i.e., it is disposed when the current instance is disposed.</param>
        public TableReader(Stream input)
        {
            Guard.NotNull(input, nameof(input));
            _input = input;
        }

        public long Position => _input.Position;

        public byte ReadTag()
        {
            var tag = _input.ReadByte();
            if (tag < 0)
            {
                throw new InvalidOperationException("No more data in stream.");
            }

            return (byte)tag;
        }

        public Task<TableHeaderRecord> ReadTableHeaderAsync()
        {
            return ReadStructureAsync<TableHeaderRecord>(skip: 0);
        }

        public Task<CommitHeaderRecord> ReadCommitHeaderAsync()
        {
            return ReadStructureAsync<CommitHeaderRecord>(skip: 1);
        }

        public Task<PutRecord> ReadPutAsync()
        {
            return ReadStructureAsync<PutRecord>(skip: 1);
        }

        public Task<TombstoneRecord> ReadTombstoneAsync()
        {
            return ReadStructureAsync<TombstoneRecord>(skip: 1);
        }

        public async Task<byte[]> ReadBytesAsync(int count)
        {
            var buffer = new byte[count];
            var idx = 0;
            while (idx < count)
            {
                var read = await _input.ReadAsync(buffer, idx, count - idx);
                if (read <= 0)
                {
                    throw new InvalidOperationException("Could not read enough bytes from stream.");
                }

                idx += read;
            }

            return buffer;
        }

        public void SkipBytes(long count)
        {
            _input.Seek(count, SeekOrigin.Current);
        }

        public Task<CommitFooterRecord> ReadCommitFooterAsync()
        {
            return ReadStructureAsync<CommitFooterRecord>(skip: 0);
        }

        public Task<TableFooterRecord> ReadTableFooterAsync()
        {
            return ReadStructureAsync<TableFooterRecord>(skip: 1);
        }

        #region IDisposable

        public void Dispose()
        {
            _input.Dispose();
            _isDisposed = true;
        }

        #endregion

        private async Task<T> ReadStructureAsync<T>(int skip)
            where T : struct
        {
            int toRead = Marshal.SizeOf<T>();
            int offset = skip;

            // Read remaining bytes from stream
            while (offset < toRead)
            {
                int read = await _input.ReadAsync(_buffer, offset, toRead - offset).ConfigureAwait(false);

                if (read <= 0)
                {
                    throw new IOException("Unexpected end of stream.");
                }

                offset += read;
            }

            // Interpret raw bytes as a struct instance
            var handle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            T structure = Marshal.PtrToStructure<T>(handle.AddrOfPinnedObject());
            handle.Free();

            return structure;
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }
    }
}

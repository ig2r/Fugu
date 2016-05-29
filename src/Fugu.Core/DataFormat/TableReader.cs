using Fugu.Common;
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Fugu.DataFormat
{
    /// <summary>
    /// Tokenizes a segment data stream.
    /// </summary>
    public class TableReader : IDisposable
    {
        private bool _isDisposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableReader"/> class.
        /// </summary>
        /// <param name="inputStream">Source stream holding elements to read. The current instance takes ownership
        /// of this stream, i.e., it is disposed when the current instance is disposed.</param>
        public TableReader(Stream inputStream)
        {
            Guard.NotNull(inputStream, nameof(inputStream));
            BaseStream = inputStream;
        }

        /// <summary>
        /// Gets a reference to the underlying input stream.
        /// </summary>
        public Stream BaseStream { get; }

        /// <summary>
        /// Gets or sets the tag that indicates the type of the next element in the stream, or null if
        /// the next element is still unknown or unavailable.
        /// </summary>
        public RecordType? CurrentTag { get; private set; }

        /// <summary>
        /// Reads a tag indicating the type of the next element in the input stream, and sets the
        /// <see cref="CurrentTag"/> property accordingly. It is the responsibility of the caller to ensure
        /// that the input stream is at a position where such a tag can be expected w.r.t. the file format.
        /// </summary>
        public void ReadTag()
        {
            ThrowIfDisposed();

            int b = BaseStream.ReadByte();
            CurrentTag = (b < 0) ? null : (RecordType?)b;
        }

        /// <summary>
        /// Reads a specified number of raw bytes from the underlying stream.
        /// </summary>
        /// <param name="count">The non-negative number of bytes to read.</param>
        /// <returns>Byte array holding the requested data.</returns>
        public async Task<byte[]> ReadBytesAsync(int count)
        {
            ThrowIfDisposed();

            byte[] buffer = new byte[count];
            int offset = 0;

            while (offset < count)
            {
                int read = await BaseStream.ReadAsync(buffer, offset, count - offset).ConfigureAwait(false);
                if (read <= 0)
                {
                    throw new IOException("Unexpected end of stream.");
                }

                offset += read;
            }

            return buffer;
        }

        public Task<HeaderRecord> ReadHeaderAsync()
        {
            ThrowIfDisposed();
            return ReadStructureAsync<HeaderRecord>(BaseStream);
        }

        public Task<FooterRecord> ReadFooterAsync()
        {
            ThrowIfDisposed();
            return ReadStructureAsync<FooterRecord>(BaseStream);
        }

        public Task<PutRecord> ReadPutAsync()
        {
            ThrowIfDisposed();
            return ReadStructureAsync<PutRecord>(BaseStream);
        }

        public Task<TombstoneRecord> ReadTombstoneAsync()
        {
            ThrowIfDisposed();
            return ReadStructureAsync<TombstoneRecord>(BaseStream);
        }

        public Task<StartDataRecord> ReadStartDataAsync()
        {
            ThrowIfDisposed();
            return ReadStructureAsync<StartDataRecord>(BaseStream);
        }

        public Task<CommitRecord> ReadCommitAsync()
        {
            ThrowIfDisposed();
            return ReadStructureAsync<CommitRecord>(BaseStream);
        }

        #region IDisposable

        public void Dispose()
        {
            BaseStream.Dispose();
            _isDisposed = true;
        }

        #endregion

        private async Task<T> ReadStructureAsync<T>(Stream stream)
            where T : struct
        {
            var buffer = new byte[Marshal.SizeOf<T>()];
            int offset = 0;

            // If a tag byte has already been loaded, copy it to the buffer
            if (CurrentTag.HasValue)
            {
                buffer[offset++] = (byte)CurrentTag.Value;
                CurrentTag = null;
            }

            // Read remaining bytes from stream
            while (offset < buffer.Length)
            {
                int read = await stream.ReadAsync(buffer, offset, buffer.Length - offset).ConfigureAwait(false);

                if (read <= 0)
                {
                    throw new IOException("Unexpected end of stream.");
                }

                offset += read;
            }

            // Interpret raw bytes as a struct instance
            var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
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

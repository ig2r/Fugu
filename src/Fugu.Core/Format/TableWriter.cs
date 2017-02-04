using Fugu.Common;
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Fugu.Format
{
    /// <summary>
    /// Provides functionality to compose a Fugu storage file by writing the building blocks of the storage format
    /// to an output stream.
    /// </summary>
    public sealed class TableWriter : IDisposable
    {
        // The default size, in bytes, of the buffer that is used to marshal C# structs to bytes.
        private const int MIN_STRUCTURE_BUFFER_SIZE = 256;

        private readonly Stream _outputStream;
        private byte[] _buffer;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableWriter"/> class.
        /// </summary>
        /// <param name="outputStream">
        /// The output stream that instances of this class will write data to. When disposing the writer instance, the
        /// stream will remain open.
        /// </param>
        public TableWriter(Stream outputStream)
        {
            Guard.NotNull(outputStream, nameof(outputStream));
            _outputStream = outputStream;
        }

        public Stream OutputStream
        {
            get { return _outputStream; }
        }

        public long Position
        {
            get { return _outputStream.Position; }
        }

        public void WriteTableHeader(long minGeneration, long maxGeneration)
        {
            var headerRecord = new TableHeaderRecord
            {
                Magic = 0xDEADBEEFL,
                FormatVersionMajor = 0,
                FormatVersionMinor = 1,
                MinGeneration = minGeneration,
                MaxGeneration = maxGeneration,
                HeaderChecksum = 0,     // TODO: Calculate for Magic, Major, Minor, generations
            };

            var data = StructureToArray(headerRecord);
            _outputStream.Write(data.Array, data.Offset, data.Count);
        }

        public void WriteTableFooter()
        {
            var footerRecord = new TableFooterRecord
            {
                Tag = TableRecordType.TableFooter,
            };

            var data = StructureToArray(footerRecord);
            _outputStream.Write(data.Array, data.Offset, data.Count);
        }

        public void WriteCommitHeader(int count)
        {
            var commitHeaderRecord = new CommitHeaderRecord
            {
                Tag = TableRecordType.CommitHeader,
                Count = count
            };

            var data = StructureToArray(commitHeaderRecord);
            _outputStream.Write(data.Array, data.Offset, data.Count);
        }

        public void WriteCommitFooter()
        {
            var commitFooterRecord = new CommitFooterRecord
            {
                CommitChecksum = 0,
            };

            var data = StructureToArray(commitFooterRecord);
            _outputStream.Write(data.Array, data.Offset, data.Count);
        }

        public void WritePut(byte[] key, int valueLength)
        {
            Guard.NotNull(key, nameof(key));

            var putRecord = new PutRecord
            {
                Tag = CommitRecordType.Put,
                KeyLength = (short)key.Length,
                ValueLength = valueLength
            };

            var data = StructureToArray(putRecord);
            _outputStream.Write(data.Array, data.Offset, data.Count);
            _outputStream.Write(key, 0, key.Length);
        }

        public void WriteTombstone(byte[] key)
        {
            Guard.NotNull(key, nameof(key));

            var tombstoneRecord = new TombstoneRecord
            {
                Tag = CommitRecordType.Tombstone,
                KeyLength = (short)key.Length
            };

            var data = StructureToArray(tombstoneRecord);
            _outputStream.Write(data.Array, data.Offset, data.Count);
            _outputStream.Write(key, 0, key.Length);
        }

        public void Write(byte[] buffer)
        {
            _outputStream.Write(buffer, 0, buffer.Length);
        }

        public Task WriteAsync(Stream sourceStream)
        {
            return sourceStream.CopyToAsync(_outputStream);
        }

        #region IDisposable

        public void Dispose()
        {
            _buffer = null;
        }

        #endregion

        private ArraySegment<byte> StructureToArray<T>(T structure)
            where T : struct
        {
            // Make sure the buffer is large enough to hold the structure
            var size = Marshal.SizeOf<T>();
            if (_buffer == null || _buffer.Length < size)
            {
                _buffer = new byte[Math.Max(size, MIN_STRUCTURE_BUFFER_SIZE)];
            }

            // Marshal to byte[] representation
            var handle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            Marshal.StructureToPtr(structure, handle.AddrOfPinnedObject(), false);
            handle.Free();

            return new ArraySegment<byte>(_buffer, 0, size);
        }
    }
}

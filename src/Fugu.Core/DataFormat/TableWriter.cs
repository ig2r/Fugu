using Fugu.Common;
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Fugu.DataFormat
{
    /// <summary>
    /// Provides functionality to compose a Fugu storage file by writing the building blocks of the storage format
    /// to an output stream.
    /// </summary>
    public sealed class TableWriter : IDisposable
    {
        // The default size, in bytes, of the buffer that is used to marshal C# structs to bytes.
        private const int MIN_STRUCTURE_BUFFER_SIZE = 256;

        private readonly BinaryWriter _binaryWriter;
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
            _binaryWriter = new BinaryWriter(outputStream, Encoding.UTF8, leaveOpen: true);
        }

        public Stream OutputStream
        {
            get { return _binaryWriter.BaseStream; }
        }

        public long Position
        {
            get { return _binaryWriter.BaseStream.Position; }
        }

        public void WriteHeader(long minGeneration, long maxGeneration)
        {
            var headerRecord = new HeaderRecord
            {
                Magic = 0xDEADBEEFL,
                FormatMajor = 0,
                FormatMinor = 1,
                MinGeneration = minGeneration,
                MaxGeneration = maxGeneration,
                HeaderChecksum = 0,     // TODO: Calculate for Magic, Major, Minor, generations

                EndOfDataPosition = 0,
                EndOfDataChecksum = 0,
            };

            var data = StructureToArray(headerRecord);
            _binaryWriter.Write(data.Array, data.Offset, data.Count);
        }

        public void WritePut(byte[] key, int valueLength)
        {
            Guard.NotNull(key, nameof(key));

            var putRecord = new PutRecord
            {
                Tag = RecordType.Put,
                KeyLength = (short)key.Length,
                ValueLength = valueLength
            };

            var data = StructureToArray(putRecord);
            _binaryWriter.Write(data.Array, data.Offset, data.Count);
            _binaryWriter.Write(key);
        }

        public void WriteTombstone(byte[] key)
        {
            Guard.NotNull(key, nameof(key));

            var tombstoneRecord = new TombstoneRecord
            {
                Tag = RecordType.Tombstone,
                KeyLength = (short)key.Length
            };

            var data = StructureToArray(tombstoneRecord);
            _binaryWriter.Write(data.Array, data.Offset, data.Count);
            _binaryWriter.Write(key);
        }

        public void WriteStartData()
        {
            var startDataRecord = new StartDataRecord { Tag = RecordType.StartData };

            var data = StructureToArray(startDataRecord);
            _binaryWriter.Write(data.Array, data.Offset, data.Count);
        }

        public void WriteCommit()
        {
            var commitRecord = new CommitRecord { Tag = RecordType.Commit, Checksum = 0L };

            var data = StructureToArray(commitRecord);
            _binaryWriter.Write(data.Array, data.Offset, data.Count);
        }

        public void Write(byte[] buffer)
        {
            _binaryWriter.Write(buffer);
        }

        public Task WriteAsync(Stream sourceStream)
        {
            return sourceStream.CopyToAsync(_binaryWriter.BaseStream);
        }

        #region IDisposable

        public void Dispose()
        {
            _binaryWriter.Dispose();
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

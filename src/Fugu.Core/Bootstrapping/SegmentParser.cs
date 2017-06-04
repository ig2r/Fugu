using Fugu.Common;
using Fugu.Format;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Fugu.Bootstrapping
{
    public class SegmentParser : IDisposable
    {
        private readonly TableReader _reader;

        private bool _consumedCurrentToken = false;
        private int _remainingElementsInCommitHeader = 0;
        private readonly Queue<PutRecord> _queuedPuts = new Queue<PutRecord>();

        public SegmentParser(Stream input)
        {
            Guard.NotNull(input, nameof(input));
            _reader = new TableReader(input);
        }

        public SegmentParserTokenType Current { get; private set; } = SegmentParserTokenType.NotStarted;

        #region IDisposable

        public void Dispose()
        {
            _reader.Dispose();
        }

        #endregion

        // Reads the next node from the input stream, or return false if no more data is available
        public async Task<bool> ReadAsync()
        {
            // If the calling code has not otherwise acted upon this token, we consume it to advance
            // the position in the underlying stream and apply its side-effects
            if (!_consumedCurrentToken)
            {
                await ConsumeCurrentTokenAsync();
            }

            // Transition to next token
            switch (Current)
            {
                case SegmentParserTokenType.NotStarted:
                    {
                        Current = SegmentParserTokenType.TableHeader;
                        _consumedCurrentToken = false;
                        return true;
                    }
                case SegmentParserTokenType.TableHeader:
                case SegmentParserTokenType.CommitFooter:
                    {
                        ExpectCommitOrTableFooter();
                        _consumedCurrentToken = false;
                        return true;
                    }
                case SegmentParserTokenType.CommitHeader:
                case SegmentParserTokenType.Put:
                case SegmentParserTokenType.Tombstone:
                case SegmentParserTokenType.Value:
                    {
                        ExpectCommitElement();
                        _consumedCurrentToken = false;
                        return true;
                    }
                case SegmentParserTokenType.TableFooter:
                    {
                        return false;
                    }
                default:
                    throw new InvalidOperationException("Unexpected token type");
            }
        }

        public Task<TableHeaderRecord> ReadTableHeaderAsync()
        {
            MarkCurrentTokenConsumed();
            return _reader.ReadTableHeaderAsync();
        }

        public Task<TableFooterRecord> ReadTableFooterAsync()
        {
            MarkCurrentTokenConsumed();
            return _reader.ReadTableFooterAsync();
        }

        public async Task<CommitHeaderRecord> ReadCommitHeaderAsync()
        {
            MarkCurrentTokenConsumed();
            var commitHeader = await _reader.ReadCommitHeaderAsync();
            _remainingElementsInCommitHeader = commitHeader.Count;
            return commitHeader;
        }

        public Task<CommitFooterRecord> ReadCommitFooterAsync()
        {
            MarkCurrentTokenConsumed();
            return _reader.ReadCommitFooterAsync();
        }

        public async Task<byte[]> ReadPutOrTombstoneKeyAsync()
        {
            MarkCurrentTokenConsumed();

            int keyLength;
            switch (Current)
            {
                case SegmentParserTokenType.Put:
                    {
                        var putRecord = await _reader.ReadPutAsync();
                        keyLength = putRecord.KeyLength;
                        _queuedPuts.Enqueue(putRecord);
                        break;
                    }
                case SegmentParserTokenType.Tombstone:
                    {
                        var tombstoneRecord = await _reader.ReadTombstoneAsync();
                        keyLength = tombstoneRecord.KeyLength;
                        break;
                    }
                default:
                    throw new InvalidOperationException("Operation not valid on current token type.");
            }

            _remainingElementsInCommitHeader--;
            var key = await _reader.ReadBytesAsync(keyLength);
            return key;
        }

        public Task<byte[]> ReadValueAsync()
        {
            MarkCurrentTokenConsumed();
            var putRecord = _queuedPuts.Dequeue();
            return _reader.ReadBytesAsync(putRecord.ValueLength);
        }

        private void MarkCurrentTokenConsumed()
        {
            if (_consumedCurrentToken)
            {
                throw new InvalidOperationException("Current token has already been consumed.");
            }

            _consumedCurrentToken = true;
        }

        private Task ConsumeCurrentTokenAsync()
        {
            switch (Current)
            {
                case SegmentParserTokenType.TableHeader:
                    return ReadTableHeaderAsync();
                case SegmentParserTokenType.CommitHeader:
                    return ReadCommitHeaderAsync();
                case SegmentParserTokenType.Put:
                case SegmentParserTokenType.Tombstone:
                    return ReadPutOrTombstoneKeyAsync();
                case SegmentParserTokenType.Value:
                    return ReadValueAsync();
                case SegmentParserTokenType.CommitFooter:
                    return ReadCommitFooterAsync();
                case SegmentParserTokenType.TableFooter:
                    return ReadTableFooterAsync();
                default:
                    MarkCurrentTokenConsumed();
                    return Task.CompletedTask;
            }
        }

        private void ExpectCommitOrTableFooter()
        {
            switch ((TableRecordType)_reader.ReadTag())
            {
                case TableRecordType.CommitHeader:
                    Current = SegmentParserTokenType.CommitHeader;
                    break;
                case TableRecordType.TableFooter:
                    Current = SegmentParserTokenType.TableFooter;
                    break;
                default:
                    // TODO: ReadTag() should signal end of stream
                    throw new InvalidOperationException("Unexpected tag or end of stream.");
            }
        }

        private void ExpectCommitElement()
        {
            if (_remainingElementsInCommitHeader > 0)
            {
                // Expect a put or tombstone
                switch ((CommitRecordType)_reader.ReadTag())
                {
                    case CommitRecordType.Put:
                        Current = SegmentParserTokenType.Put;
                        break;
                    case CommitRecordType.Tombstone:
                        Current = SegmentParserTokenType.Tombstone;
                        break;
                    default:
                        throw new InvalidOperationException("Unexpected tag or end of stream.");
                }
            }
            else if (_queuedPuts.Count > 0)
            {
                // Expect a value element
                Current = SegmentParserTokenType.Value;
            }
            else
            {
                // Expect commit footer
                Current = SegmentParserTokenType.CommitFooter;
            }
        }
    }
}

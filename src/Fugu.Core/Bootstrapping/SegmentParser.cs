using Fugu.Common;
using Fugu.IO;
using Fugu.IO.Records;
using System;
using System.Collections.Generic;

namespace Fugu.Bootstrapping
{
    public class SegmentParser
    {
        private readonly TableReader _reader;

        private bool _consumedCurrentToken = false;
        private int _remainingElementsInCommitHeader = 0;
        private readonly Queue<PutRecord> _queuedPuts = new Queue<PutRecord>();

        public SegmentParser(TableReader reader)
        {
            Guard.NotNull(reader, nameof(reader));
            _reader = reader;
        }

        public SegmentParserTokenType Current { get; private set; } = SegmentParserTokenType.NotStarted;

        // Reads the next node from the input stream, or return false if no more data is available
        public bool Read()
        {
            // If the calling code has not otherwise acted upon this token, we consume it to advance
            // the position in the underlying stream and apply its side-effects
            if (!_consumedCurrentToken)
            {
                ConsumeCurrentToken();
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

        public TableHeaderRecord ReadTableHeader()
        {
            MarkCurrentTokenConsumed();
            return _reader.ReadTableHeader();
        }

        public TableFooterRecord ReadTableFooter()
        {
            MarkCurrentTokenConsumed();
            return _reader.ReadTableFooter();
        }

        public CommitHeaderRecord ReadCommitHeader()
        {
            MarkCurrentTokenConsumed();
            var commitHeader = _reader.ReadCommitHeader();
            _remainingElementsInCommitHeader = commitHeader.Count;
            return commitHeader;
        }

        public CommitFooterRecord ReadCommitFooter()
        {
            MarkCurrentTokenConsumed();
            return _reader.ReadCommitFooter();
        }

        public byte[] ReadPutOrTombstoneKey()
        {
            MarkCurrentTokenConsumed();

            int keyLength;
            switch (Current)
            {
                case SegmentParserTokenType.Put:
                    {
                        var putRecord = _reader.ReadPut();
                        keyLength = putRecord.KeyLength;
                        _queuedPuts.Enqueue(putRecord);
                        break;
                    }
                case SegmentParserTokenType.Tombstone:
                    {
                        var tombstoneRecord = _reader.ReadTombstone();
                        keyLength = tombstoneRecord.KeyLength;
                        break;
                    }
                default:
                    throw new InvalidOperationException("Operation not valid on current token type.");
            }

            _remainingElementsInCommitHeader--;
            var key = _reader.ReadBytes(keyLength).ToArray();
            return key;
        }

        public byte[] ReadValue()
        {
            MarkCurrentTokenConsumed();
            var putRecord = _queuedPuts.Dequeue();
            return _reader.ReadBytes(putRecord.ValueLength).ToArray();
        }

        private void MarkCurrentTokenConsumed()
        {
            if (_consumedCurrentToken)
            {
                throw new InvalidOperationException("Current token has already been consumed.");
            }

            _consumedCurrentToken = true;
        }

        private void ConsumeCurrentToken()
        {
            switch (Current)
            {
                case SegmentParserTokenType.TableHeader:
                    ReadTableHeader();
                    break;
                case SegmentParserTokenType.CommitHeader:
                    ReadCommitHeader();
                    break;
                case SegmentParserTokenType.Put:
                case SegmentParserTokenType.Tombstone:
                    ReadPutOrTombstoneKey();
                    break;
                case SegmentParserTokenType.Value:
                    ReadValue();
                    break;
                case SegmentParserTokenType.CommitFooter:
                    ReadCommitFooter();
                    break;
                case SegmentParserTokenType.TableFooter:
                    ReadTableFooter();
                    break;
                default:
                    MarkCurrentTokenConsumed();
                    break;
            }
        }

        private void ExpectCommitOrTableFooter()
        {
            switch ((TableRecordType)_reader.GetTag())
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
                switch ((CommitRecordType)_reader.GetTag())
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

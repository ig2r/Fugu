using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Common
{
    /// <summary>
    /// A continguous range of bytes.
    /// </summary>
    /// <typeparam name="TImpl">The concrete type implementing this interface.</typeparam>
    public interface IByteSpan<TImpl> where TImpl : struct
    {
        long Length { get; }
        byte this[long index] { get; set; }

        TImpl Slice(long startIndex);
        TImpl Slice(long startIndex, long length);

        T Read<T>() where T : struct;
        void CopyTo(byte[] destinationArray);

        TImpl Write<T>(ref T item) where T : struct;
        TImpl Write(byte[] sourceArray, int sourceIndex, int length);
    }
}

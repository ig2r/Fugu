using System.Collections.Generic;

namespace Fugu.Common
{
    /// <summary>
    /// Compares byte arrays lexicographically.
    /// </summary>
    public sealed class ByteArrayComparer : IComparer<byte[]>
    {
        #region IComparer<byte[]>

        public int Compare(byte[] x, byte[] y)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }

            for (int i = 0; i < x.Length && i < y.Length; i++)
            {
                if (x[i] != y[i])
                {
                    return x[i].CompareTo(y[i]);
                }
            }

            return x.Length.CompareTo(y.Length);
        }

        #endregion
    }
}

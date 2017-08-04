using System.Collections;
using System.Collections.Generic;

namespace Fugu.Common
{
    /// <summary>
    /// Tests byte arrays for equality based on their contents.
    /// </summary>
    public sealed class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
    {
        #region IEqualityComparer<byte[]>

        public bool Equals(byte[] x, byte[] y)
        {
            return StructuralComparisons.StructuralEqualityComparer.Equals(x, y);
        }

        public int GetHashCode(byte[] obj)
        {
            return StructuralComparisons.StructuralEqualityComparer.GetHashCode(obj);
        }

        #endregion
    }
}

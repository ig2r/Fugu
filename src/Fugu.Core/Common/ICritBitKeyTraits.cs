namespace Fugu.Common
{
    /// <summary>
    /// Guides the transformation of key values (of type <typeparamref name="TKey"/>) into bit strings that can be
    /// used as keys in a crit-bit tree structure.
    /// </summary>
    /// <typeparam name="TKey">The type of input key used in a crit-bit tree instance.</typeparam>
    public interface ICritBitKeyTraits<in TKey>
    {
        /// <summary>
        /// Gets the bit (0 or 1) at a given zero-based bit position in some key. Note that for crit-bit trees that store
        /// keys of different lengths, keys should be treated as having an infinite suffix of zeroes.
        /// </summary>
        /// <param name="key">Key to retrieve the indicated bit from.</param>
        /// <param name="bitIndex">Index of the bit in the key to retrieve.</param>
        /// <returns>Bit value.</returns>
        int GetBit(TKey key, int bitIndex);

        /// <summary>
        /// Given two keys, finds the index of the first bit in which these keys differ, or int.MaxValue if they are equal.
        /// </summary>
        /// <param name="key1">First operand.</param>
        /// <param name="key2">Second operand.</param>
        /// <returns>Zero-based index of the first differing bit, or int.MaxValue if equal.</returns>
        int FindCritBitIndex(TKey key1, TKey key2);

        /// <summary>
        /// Gets a value indicating whether two keys have equalling bit string representations.
        /// </summary>
        /// <param name="key1">First operand.</param>
        /// <param name="key2">Second operand.</param>
        /// <returns>True if the given keys are equal, false otherwise.</returns>
        bool Equals(TKey key1, TKey key2);
    }
}

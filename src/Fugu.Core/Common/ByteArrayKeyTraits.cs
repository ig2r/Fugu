using System;

namespace Fugu.Common
{
    /// <summary>
    /// Defines a mapping of byte strings to prefix-free bit strings with the same lexicographic ordering.
    /// Since keys that are made up of byte strings are not generally prefix-free (i.e., one key might be a prefix of
    /// another key in the dictionary), this implementation depends on a representation of such byte keys where each
    /// key byte is turned into 9 bits, with the last bit equal to 1 if there are more bytes in the string, or 0 if
    /// it is the last byte.
    /// </summary>
    public struct ByteArrayKeyTraits : ICritBitKeyTraits<byte[]>
    {
        public int FindCritBitIndex(byte[] key1, byte[] key2)
        {
            int maxLength = Math.Min(key1.Length, key2.Length);
            for (int i = 0; i < maxLength; i++)
            {
                int xor = key1[i] ^ key2[i];
                if (xor != 0)
                {
                    // Find position of first '1' by counting leading zeroes
                    int zeroes = 0;
                    if ((xor & 0xF0) == 0) zeroes += 4;
                    if ((xor << zeroes & 0xC0) == 0) zeroes += 2;
                    if ((xor << zeroes & 0x80) == 0) zeroes++;

                    return i * 9 + 1 + zeroes;
                }
            }

            if (key1.Length == key2.Length)
            {
                // Keys are equal
                return int.MaxValue;
            }

            // One key is strict prefix of the other, difference is in absent terminating 0-byte
            return maxLength * 9;
        }

        public int GetBit(byte[] key, int bitIndex)
        {
            if (bitIndex >= key.Length * 9) return 0;

            int i = bitIndex % 9;
            return i == 0 ? 1 : (key[bitIndex / 9] >> (8 - i)) & 1;
        }

        public bool Equals(byte[] key1, byte[] key2)
        {
            if (ReferenceEquals(key1, key2))
            {
                return true;
            }

            if (key1.Length != key2.Length)
            {
                return false;
            }

            for (int i = 0; i < key1.Length; i++)
            {
                if (key1[i] != key2[i])
                {
                    return false;
                }
            }

            return true;
        }
    }
}

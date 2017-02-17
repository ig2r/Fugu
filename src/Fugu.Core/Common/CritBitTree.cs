using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Fugu.Common
{
    /// <summary>
    /// An immutable dictionary implemented as a crit-bit tree.
    /// </summary>
    /// <typeparam name="TKeyTraits">Traits type that lets operations treat keys as a sequence of bits.</typeparam>
    /// <typeparam name="TKey">The type of keys to store.</typeparam>
    /// <typeparam name="TValue">The type of the values to store.</typeparam>
    /// <remarks>
    /// Crit-bit trees store a prefix-free set of bit strings in a binary tree where each internal node represents
    /// a bit position in which two keys differ, and each leaf node represents a (key, value) pair stored in the tree.
    /// </remarks>
    public sealed class CritBitTree<TKeyTraits, TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
        where TKeyTraits : ICritBitKeyTraits<TKey>, new()
    {
        private readonly static CritBitTree<TKeyTraits, TKey, TValue> _empty = new CritBitTree<TKeyTraits, TKey, TValue>(null);
        private readonly static TKeyTraits _traits = new TKeyTraits();

        private readonly Node _root;

        private CritBitTree(Node root)
        {
            _root = root;
        }

        public static CritBitTree<TKeyTraits, TKey, TValue> Empty
        {
            get { return _empty; }
        }

        /// <summary>
        /// Gets the value associated with a given key.
        /// </summary>
        /// <param name="key">The key to look up.</param>
        /// <returns>The associated value.</returns>
        public TValue this[TKey key]
        {
            get
            {
                TValue value;
                if (!TryGetValue(key, out value))
                {
                    throw new KeyNotFoundException();
                }

                return value;
            }
        }

        /// <summary>
        /// Retrieves the value associated with a given key if it is in the dictionary.
        /// </summary>
        /// <param name="key">The key to look up.</param>
        /// <param name="value">The value to assign on success, or set to default if the key is not found.</param>
        /// <returns>A value indicating whether the given key was found in the dictionary.</returns>
        public bool TryGetValue(TKey key, out TValue value)
        {
            if (_root != null)
            {
                var leaf = _root.SeekToLeaf(key);
                if (_traits.Equals(key, leaf.Key))
                {
                    value = leaf.Value;
                    return true;
                }
            }

            value = default(TValue);
            return false;
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetItems(TKey startKey)
        {
            if (_root == null)
            {
                return Enumerable.Empty<KeyValuePair<TKey, TValue>>();
            }

            // Trace down the tree towards a payload node via the signpost bits and find the first bit that differs
            var leaf = _root.SeekToLeaf(startKey);
            int critBitIndex = _traits.FindCritBitIndex(startKey, leaf.Key);

            // Trace down once more from the root, building up a stack of nodes that we'll need to yield items from
            Node n = _root;
            var stack = new Stack<Node>();
            while (n is Node.Branch branch)
            {
                // Stop if this branch is at a position past the crit-bit
                if (branch.BitIndex >= critBitIndex)
                    break;

                // Otherwise, descend farther
                int bit = _traits.GetBit(startKey, branch.BitIndex);
                if (bit == 0)
                {
                    stack.Push(branch.One);
                    n = branch.Zero;
                }
                else
                {
                    n = branch.One;
                }
            }

            // Decide what to do with the node where we stopped the descent; depending on how the bits at the crit-bit
            // positions in our key and that node compare, we either include it or discard it completely
            if (_traits.GetBit(startKey, critBitIndex) <= _traits.GetBit(leaf.Key, critBitIndex))
            {
                stack.Push(n);
            }

            // Yield items from stack in pre-order traversal
            return EnumerateStack(stack);
        }

        public CritBitTree<TKeyTraits, TKey, TValue> SetItem(TKey key, TValue value)
        {
            if (_root == null)
            {
                return new CritBitTree<TKeyTraits, TKey, TValue>(new Node.Payload(key, value));
            }

            // Walk down the tree and find a terminal payload node
            var payload = _root.SeekToLeaf(key);

            // Find the index of the first bit where key and this terminal node differ; note that this
            // index might be past the end of the key if both items are identical
            int critBitIndex = _traits.FindCritBitIndex(key, payload.Key);

            // Now do the actual insertion at the crit-bit index we determined
            var newRoot = _root.SetItem(key, value, critBitIndex);
            return new CritBitTree<TKeyTraits, TKey, TValue>(newRoot);
        }

        public CritBitTree<TKeyTraits, TKey, TValue> Remove(TKey key)
        {
            if (_root == null)
            {
                return this;
            }

            var newRoot = _root.Remove(key);
            return new CritBitTree<TKeyTraits, TKey, TValue>(newRoot);
        }

        #region IEnumerable<KeyValuePair<TKey, TValue>>

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            if (_root == null)
            {
                return Enumerable.Empty<KeyValuePair<TKey, TValue>>().GetEnumerator();
            }

            var stack = new Stack<Node>();
            stack.Push(_root);
            return EnumerateStack(stack).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        private IEnumerable<KeyValuePair<TKey, TValue>> EnumerateStack(Stack<Node> stack)
        {
            // Yield items from stack in pre-order traversal
            while (stack.Count > 0)
            {
                var node = stack.Pop();
                if (node is Node.Branch branch)
                {
                    stack.Push(branch.One);
                    stack.Push(branch.Zero);
                }
                else
                {
                    var payload = (Node.Payload)node;
                    yield return new KeyValuePair<TKey, TValue>(payload.Key, payload.Value);
                }
            }
        }

        /// <summary>
        /// Algebraic data type (ADT) to represent nodes in a crit-bit tree.
        /// </summary>
        private abstract class Node
        {
            private Node() { }

            public abstract Payload SeekToLeaf(TKey key);
            public abstract Node SetItem(TKey key, TValue value, int critBitIndex);
            public abstract Node Remove(TKey key);

            protected Node InsertAsSibling(TKey key, TValue value, int critBitIndex)
            {
                // Creates a branch node on top of the current node and inserts the given key as a new sibling
                var payload = new Payload(key, value);
                int critBit = _traits.GetBit(key, critBitIndex);
                return critBit == 0
                    ? new Branch(critBitIndex, payload, this)
                    : new Branch(critBitIndex, this, payload);
            }

            public sealed class Branch : Node
            {
                public Branch(int bitIndex, Node zero, Node one)
                {
                    BitIndex = bitIndex;
                    Zero = zero;
                    One = one;
                }

                public int BitIndex { get; }
                public Node One { get; }
                public Node Zero { get; }

                public override Payload SeekToLeaf(TKey key)
                {
                    return _traits.GetBit(key, BitIndex) == 0
                        ? Zero.SeekToLeaf(key)
                        : One.SeekToLeaf(key);
                }

                public override Node SetItem(TKey key, TValue value, int critBitIndex)
                {
                    if (critBitIndex < BitIndex)
                    {
                        // This node is past the crit-bit position, so insert a new branch on
                        // top of the current node
                        return InsertAsSibling(key, value, critBitIndex);
                    }

                    // Continue descent
                    return _traits.GetBit(key, BitIndex) == 0
                        ? new Branch(BitIndex, Zero.SetItem(key, value, critBitIndex), One)
                        : new Branch(BitIndex, Zero, One.SetItem(key, value, critBitIndex));
                }

                public override Node Remove(TKey key)
                {
                    int branchBit = _traits.GetBit(key, BitIndex);
                    if (branchBit == 0)
                    {
                        var newChild = Zero.Remove(key);
                        return newChild == null ? One : new Branch(BitIndex, newChild, One);
                    }
                    else
                    {
                        var newChild = One.Remove(key);
                        return newChild == null ? Zero : new Branch(BitIndex, Zero, newChild);
                    }
                }
            }

            public sealed class Payload : Node
            {
                public Payload(TKey key, TValue value)
                {
                    Key = key;
                    Value = value;
                }

                public TKey Key { get; }
                public TValue Value { get; }

                public override Payload SeekToLeaf(TKey key)
                {
                    return this;
                }

                public override Node SetItem(TKey key, TValue value, int critBitIndex)
                {
                    // If the new key is equal to the key in this payload node, we replace the entire node;
                    // otherwise, we insert the new key as a sibling to this node
                    return critBitIndex == int.MaxValue
                        ? new Payload(key, value)
                        : InsertAsSibling(key, value, critBitIndex);
                }

                public override Node Remove(TKey key)
                {
                    return _traits.Equals(key, Key) ? null : this;
                }
            }
        }
    }
}

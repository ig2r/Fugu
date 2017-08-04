using System.Collections;
using System.Collections.Generic;

namespace Fugu.Common
{
    public sealed class AaTree<TValue> : IEnumerable<KeyValuePair<byte[], TValue>>
    {
        private static readonly ByteArrayComparer _keyComparer = new ByteArrayComparer();
        private readonly Node _root;

        private AaTree(Node root)
        {
            _root = root;
        }

        public static AaTree<TValue> Empty
        {
            get => new AaTree<TValue>(null);
        }

        public TValue this[byte[] key]
        {
            get
            {
                Guard.NotNull(key, nameof(key));
                if (!TryGetValue(_root, key, out var value))
                {
                    throw new KeyNotFoundException();
                }

                return value;
            }
        }

        public bool TryGetValue(byte[] key, out TValue value)
        {
            Guard.NotNull(key, nameof(key));
            return TryGetValue(_root, key, out value);
        }

        #region IEnumerable<KeyValuePair<byte[], TValue>>

        public IEnumerator<KeyValuePair<byte[], TValue>> GetEnumerator()
        {
            if (_root == null)
            {
                yield break;
            }

            var stack = new Stack<Node>();

            // Push elements along left spine
            for (var n = _root; n != null; n = n.Left)
            {
                stack.Push(n);
            }

            while (stack.Count > 0)
            {
                var node = stack.Pop();
                yield return new KeyValuePair<byte[], TValue>(node.Key, node.Value);

                for (var n = node.Right; n != null; n = n.Left)
                {
                    stack.Push(n);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        private static bool TryGetValue(Node node, byte[] key, out TValue value)
        {
            while (node != null)
            {
                var cmp = _keyComparer.Compare(key, node.Key);
                if (cmp == 0)
                {
                    value = node.Value;
                    return true;
                }
                else if (cmp < 0)
                {
                    node = node.Left;
                }
                else
                {
                    node = node.Right;
                }
            }

            value = default(TValue);
            return false;
        }

        #region Builder

        public sealed class Builder
        {
            private Node _root;

            public TValue this[byte[] key]
            {
                get
                {
                    Guard.NotNull(key, nameof(key));
                    if (!AaTree<TValue>.TryGetValue(_root, key, out var value))
                    {
                        throw new KeyNotFoundException();
                    }

                    return value;
                }

                set
                {
                    Guard.NotNull(key, nameof(key));
                    _root = Insert(_root, key, value);
                }
            }

            public bool TryGetValue(byte[] key, out TValue value)
            {
                Guard.NotNull(key, nameof(key));
                return AaTree<TValue>.TryGetValue(_root, key, out value);
            }

            public AaTree<TValue> ToImmutable()
            {
                return new AaTree<TValue>(_root);
            }

            public bool Remove(byte[] key)
            {
                Guard.NotNull(key, nameof(key));
                _root = Remove(_root, key, out var removed);
                return removed;
            }

            private static Node Insert(Node node, byte[] key, TValue value)
            {
                if (node == null)
                {
                    // Insert as leaf
                    return new Node(key, value);
                }

                // Proceed to insertion point
                var cmp = _keyComparer.Compare(key, node.Key);
                if (cmp < 0)
                {
                    var l = Insert(node.Left, key, value);
                    node = node.Modify(l, node.Right, node.Level);
                }
                else if (cmp > 0)
                {
                    var r = Insert(node.Right, key, value);
                    node = node.Modify(node.Left, r, node.Level);
                }
                else
                {
                    // Replace; no structural change, so we can skip skew/split
                    return new Node(node.Left, node.Right, node.Level, key, value);
                }

                // Fix problems on our way up
                node = Skew(node);
                node = Split(node);

                return node;
            }

            private static Node Remove(Node node, byte[] key, out bool removed)
            {
                if (node == null)
                {
                    removed = false;
                    return null;
                }

                var cmp = _keyComparer.Compare(key, node.Key);
                if (cmp == 0)
                {
                    if (node.Left != null && node.Right != null)
                    {
                        // Find in-order predecessor which will be a leaf...
                        var heir = node.Left;
                        while (heir.Right != null)
                        {
                            heir = heir.Right;
                        }

                        // ..., copy it into this spot, and remove the original predecessor
                        var l = Remove(node.Left, heir.Key, out removed);
                        node = new Node(l, node.Right, node.Level, heir.Key, heir.Value);
                    }
                    else
                    {
                        // At least one child is null
                        if (node.Left == null)
                        {
                            // Note that node.Right might be null also
                            node = node.Right;
                        }
                        else
                        {
                            node = node.Left;
                        }

                        removed = true;
                    }
                }
                else if (cmp < 0)
                {
                    var l = Remove(node.Left, key, out removed);
                    node = node.Modify(l, node.Right, node.Level);
                }
                else
                {
                    var r = Remove(node.Right, key, out removed);
                    node = node.Modify(node.Left, r, node.Level);
                }

                // Fix level breaks on our way up
                if (node != null && (node.Left?.Level < node.Level - 1 || node.Right?.Level < node.Level - 1))
                {
                    node = node.Modify(node.Left, node.Right, node.Level - 1);
                    if (node.Right?.Level > node.Level)
                    {
                        var r = node.Right;
                        r = r.Modify(r.Left, r.Right, node.Level);
                        node = node.Modify(node.Left, r, node.Level);
                    }

                    // Skew node, node.Right, node.Right.Right
                    {
                        node = Skew(node);
                        var r = Skew(node.Right);
                        if (r != null)
                        {
                            var rr = r.Right;
                            rr = Skew(rr);
                            r = r.Modify(r.Left, rr, r.Level);
                        }

                        node = node.Modify(node.Left, r, node.Level);
                    }

                    // Split node, node.Right
                    node = Split(node);
                    node = node.Modify(node.Left, Split(node.Right), node.Level);
                }

                return node;
            }

            // Conditional right rotation to fix a left horizontal link
            private static Node Skew(Node node)
            {
                if (node == null || node.Left == null || node.Level != node.Left.Level)
                {
                    return node;
                }

                var l = node.Left;
                node = node.Modify(l.Right, node.Right, node.Level);
                l = l.Modify(l.Left, node, l.Level);
                return l;
            }

            // Conditional left rotation to fix a double right horizontal link
            private static Node Split(Node node)
            {
                if (node == null || node.Right == null || node.Right.Right == null || node.Level != node.Right.Right.Level)
                {
                    return node;
                }

                var r = node.Right;
                node = node.Modify(node.Left, r.Left, node.Level);
                r = r.Modify(node, r.Right, r.Level + 1);
                return r;
            }
        }

        #endregion

        #region Node

        private class Node
        {
            public readonly Node Left;
            public readonly Node Right;
            public readonly int Level;

            public readonly byte[] Key;
            public readonly TValue Value;

            public Node(byte[] key, TValue value)
                : this(null, null, 1, key, value)
            {
            }

            public Node(Node left, Node right, int level, byte[] key, TValue value)
            {
                Left = left;
                Right = right;
                Level = level;
                Key = key;
                Value = value;
            }

            public Node Modify(Node left, Node right, int level)
            {
                return new Node(left, right, level, Key, Value);
            }
        }

        #endregion
    }
}

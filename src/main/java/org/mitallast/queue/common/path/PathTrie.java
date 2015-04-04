package org.mitallast.queue.common.path;

import gnu.trove.impl.HashFunctions;
import org.mitallast.queue.common.strings.CharSequenceReference;
import org.mitallast.queue.common.strings.QueryStringDecoder;

import java.util.Map;

@SuppressWarnings("unchecked")
public class PathTrie<TrieType> {

    private final TrieNode<TrieType> root;
    private final char separator;

    public PathTrie() {
        this('/');
    }

    public PathTrie(char separator) {
        this.separator = separator;
        root = new TrieNode<>(String.valueOf(separator), null);
    }

    public void insert(String path, TrieType value) {
        CharSequence[] parts = CharSequenceReference.splitStringToArray(path, separator);
        if (parts.length == 0) {
            root.value = value;
            return;
        }
        int index = 0;
        // supports initial delimiter.
        if (parts.length > 0 && parts[0].length() == 0) {
            index = 1;
        }
        root.insert(parts, index, value);
    }

    public TrieType retrieve(CharSequence path, Map<String, CharSequence> params) {
        if (path.length() == 0 || path.length() == 1 && path.charAt(0) == separator) {
            return root.value;
        }
        CharSequence[] strings = CharSequenceReference.splitStringToArray(path, separator);
        if (strings.length == 0) {
            return root.value;
        }
        return root.retrieve(strings, 0, params);
    }

    public void prettyPrint() {
        root.prettyPrint(0);
    }

    private static class TrieNode<NodeType> {
        private final static float loadFactor = 0.7f;
        private final static TrieNode[] empty = new TrieNode[0];
        private final CharSequence key;
        private final int keyHash;
        private TrieNode<NodeType>[] children;
        private TrieNode<NodeType>[] childrenNamedWildcard;
        private NodeType value;
        private String namedWildcard;

        public TrieNode(CharSequence key, NodeType value) {
            this.value = value;
            this.children = new TrieNode[HashFunctions.fastCeil(42 / loadFactor)];
            this.childrenNamedWildcard = empty;
            this.key = key;
            this.keyHash = hash(key);
            if (isNamedWildcard(key)) {
                updateKeyWithNamedWildcard(key);
            } else {
                namedWildcard = null;
            }
        }

        public void updateKeyWithNamedWildcard(CharSequence key) {
            int len = key.length();
            namedWildcard = key.subSequence(1, len - 1).toString();
        }

        private boolean isNamedWildcard(CharSequence key) {
            return key.charAt(0) == '{'
                && key.charAt(key.length() - 1) == '}';
        }

        public void insert(CharSequence[] path, int index, NodeType value) {
            synchronized (this) {
                if (index >= path.length) {
                    return;
                }

                final CharSequence token = path[index];

                TrieNode<NodeType> node = null;
                if (isNamedWildcard(token)) {
                    for (TrieNode<NodeType> child : childrenNamedWildcard) {
                        if (CharSequenceReference.equals(child.key, token)) {
                            node = child;
                            break;
                        }
                    }
                    if (node == null) {
                        node = new TrieNode<>(token, null);
                        TrieNode<NodeType>[] tmp = new TrieNode[childrenNamedWildcard.length + 1];
                        System.arraycopy(childrenNamedWildcard, 0, tmp, 0, childrenNamedWildcard.length);
                        tmp[tmp.length - 1] = node;
                        childrenNamedWildcard = tmp;
                    }
                } else {
                    int keyIndex = insertKey(token);
                    if (keyIndex >= 0) {
                        node = children[keyIndex];
                    }
                    if (node == null) {
                        node = new TrieNode<>(token, null);
                        children[keyIndex] = node;
                    }
                }

                if (index == (path.length - 1)) {
                    node.value = value;
                } else {
                    node.insert(path, index + 1, value);
                }
            }
        }

        private int insertKey(CharSequence sequence) {
            final TrieNode<NodeType>[] children = this.children;
            final int size = children.length;
            final int hash = hash(sequence);
            int index = hash % size;

            if (children[index] == null) {
                return index;
            } else if (children[index].keyEquals(sequence, hash)) {
                return index;
            }

            final int loopIndex = index;
            final int probe = 1 + (hash % (size - 2));
            do {
                index = index - probe;
                if (index < 0) {
                    index += size;
                }

                if (children[index] == null) {
                    return index;
                } else if (children[index].keyEquals(sequence, hash)) {
                    return index;
                }
                // Detect loop
            } while (index != loopIndex);

            // if full, rehash it
            this.children = new TrieNode[HashFunctions.fastCeil(size * 2 / loadFactor)];
            for (TrieNode<NodeType> child : children) {
                int i = insertKey(child.key);
                this.children[i] = child;
            }
            return insertKey(sequence);
        }

        private int indexKey(CharSequence sequence) {
            final TrieNode<NodeType>[] children = this.children;
            final int size = children.length;
            final int hash = hash(sequence);
            int index = hash % size;

            if (children[index] == null) {
                return -1;
            } else if (children[index].keyEquals(sequence, hash)) {
                return index;
            }
            final int loopIndex = index;
            final int probe = 1 + (hash % (size - 2));
            do {
                index = index - probe;
                if (index < 0) {
                    index += size;
                }

                if (children[index] == null) {
                    return -1;
                } else if (children[index].keyEquals(sequence, hash)) {
                    return index;
                }
                // Detect loop
            } while (index != loopIndex);
            return -1;
        }

        private boolean keyEquals(CharSequence sequence, int hash) {
            return hash == keyHash
                && CharSequenceReference.equals(sequence, key);
        }

        private int hash(CharSequence sequence) {
            if (sequence == null) return 0;
            int len = sequence.length();
            if (len == 0) return 0;
            int hash = 0;
            for (int i = 0, end = Math.min(10, len); i < end; i++) {
                hash = hash * 31 ^ sequence.charAt(i) & 31;
            }
            return hash & 0x7fffffff;
        }

        private boolean isNamedWildcard() {
            return namedWildcard != null;
        }

        public NodeType retrieve(CharSequence[] path, int index, Map<String, CharSequence> params) {
            if (index >= path.length) {
                return null;
            }
            final CharSequence token = path[index];
            int keyIndex = indexKey(token);
            if (keyIndex >= 0) {
                TrieNode<NodeType> node = children[keyIndex];
                final NodeType res;
                if (index == (path.length - 1)) {
                    res = node.value;
                } else {
                    res = node.retrieve(path, index + 1, params);
                }
                if (res != null) {
                    put(params, node, token);
                    return res;
                }
            }
            for (TrieNode<NodeType> child : childrenNamedWildcard) {
                final NodeType res;
                if (index == (path.length - 1)) {
                    res = child.value;
                } else {
                    res = child.retrieve(path, index + 1, params);
                }
                if (res != null) {
                    put(params, child, token);
                    return res;
                }
            }
            return null;
        }

        private void put(Map<String, CharSequence> params, TrieNode<NodeType> node, CharSequence value) {
            if (node.isNamedWildcard()) {
                params.put(node.namedWildcard, QueryStringDecoder.decodeComponent(value));
            }
        }

        private void prettyPrint(int level) {
            for (int i = 0; i < level; i++) {
                System.out.print("  ");
            }
            System.out.println(" |-[" + key + "(" + value + ")]");
            for (TrieNode<NodeType> child : children) {
                if (child != null) {
                    child.prettyPrint(level + 1);
                }
            }
            for (TrieNode<NodeType> child : childrenNamedWildcard) {
                child.prettyPrint(level + 1);
            }
        }
    }
}

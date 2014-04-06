package org.mitallast.queue.common.path;

import org.mitallast.queue.common.Strings;

import java.util.HashMap;
import java.util.Map;

public class PathTrie<TrieType> {

    public static final Decoder NO_DECODER = new Decoder() {
        @Override
        public String decode(String value) {
            return value;
        }
    };
    private final Decoder decoder;
    private final TrieNode<TrieType> root;
    private final char separator;
    private TrieType rootValue;

    public PathTrie() {
        this('/', "*", NO_DECODER);
    }

    public PathTrie(Decoder decoder) {
        this('/', "*", decoder);
    }

    public PathTrie(char separator, String wildcard, Decoder decoder) {
        this.decoder = decoder;
        this.separator = separator;
        root = new TrieNode<>(new String(new char[]{separator}), null, null, wildcard);
    }

    public void insert(String path, TrieType value) {
        String[] strings = Strings.splitStringToArray(path, separator);
        if (strings.length == 0) {
            rootValue = value;
            return;
        }
        int index = 0;
        // supports initial delimiter.
        if (strings.length > 0 && strings[0].isEmpty()) {
            index = 1;
        }
        root.insert(strings, index, value);
    }

    public TrieType retrieve(String path) {
        return retrieve(path, null);
    }

    public TrieType retrieve(String path, Map<String, String> params) {
        if (path.length() == 0) {
            return rootValue;
        }
        String[] strings = Strings.splitStringToArray(path, separator);
        if (strings.length == 0) {
            return rootValue;
        }
        int index = 0;
        // supports initial delimiter.
        if (strings.length > 0 && strings[0].isEmpty()) {
            index = 1;
        }
        return root.retrieve(strings, index, params);
    }

    public static interface Decoder {
        String decode(String value);
    }

    public class TrieNode<NodeType> {
        private final String wildcard;
        private final TrieNode parent;
        private transient String key;
        private transient NodeType value;
        private boolean isWildcard;
        private transient String namedWildcard;
        private Map<String, TrieNode<NodeType>> children;

        public TrieNode(String key, NodeType value, TrieNode parent, String wildcard) {
            this.key = key;
            this.wildcard = wildcard;
            this.isWildcard = (key.equals(wildcard));
            this.parent = parent;
            this.value = value;
            this.children = new HashMap<>();
            if (isNamedWildcard(key)) {
                namedWildcard = key.substring(key.indexOf('{') + 1, key.indexOf('}'));
            } else {
                namedWildcard = null;
            }
        }

        public void updateKeyWithNamedWildcard(String key) {
            this.key = key;
            namedWildcard = key.substring(key.indexOf('{') + 1, key.indexOf('}'));
        }

        public boolean isWildcard() {
            return isWildcard;
        }

        public synchronized void addChild(TrieNode<NodeType> child) {
            children.put(child.key, child);
        }

        public TrieNode getChild(String key) {
            return children.get(key);
        }

        public synchronized void insert(String[] path, int index, NodeType value) {
            if (index >= path.length) {
                return;
            }

            String token = path[index];
            String key = token;
            if (isNamedWildcard(token)) {
                key = wildcard;
            }
            TrieNode<NodeType> node = children.get(key);
            if (node == null) {
                if (index == (path.length - 1)) {
                    node = new TrieNode<NodeType>(token, value, this, wildcard);
                } else {
                    node = new TrieNode<NodeType>(token, null, this, wildcard);
                }
                children.put(key, node);
            } else {
                if (isNamedWildcard(token)) {
                    node.updateKeyWithNamedWildcard(token);
                }

                // in case the target(last) node already exist but without a value
                // than the value should be updated.
                if (index == (path.length - 1)) {
                    assert (node.value == null || node.value == value);
                    if (node.value == null) {
                        node.value = value;
                    }
                }
            }

            node.insert(path, index + 1, value);
        }

        private boolean isNamedWildcard(String key) {
            return key.indexOf('{') != -1 && key.indexOf('}') != -1;
        }

        private String namedWildcard() {
            return namedWildcard;
        }

        private boolean isNamedWildcard() {
            return namedWildcard != null;
        }

        public NodeType retrieve(String[] path, int index, Map<String, String> params) {
            if (index >= path.length) {
                return null;
            }

            String token = path[index];
            TrieNode<NodeType> node = children.get(token);
            boolean usedWildcard;
            if (node == null) {
                node = children.get(wildcard);
                if (node == null) {
                    return null;
                }
                usedWildcard = true;
            } else {
                usedWildcard = token.equals(wildcard);
            }

            put(params, node, token);

            if (index == (path.length - 1)) {
                return node.value;
            }

            NodeType res = node.retrieve(path, index + 1, params);
            if (res == null && !usedWildcard) {
                node = children.get(wildcard);
                if (node != null) {
                    put(params, node, token);
                    res = node.retrieve(path, index + 1, params);
                }
            }

            return res;
        }

        private void put(Map<String, String> params, TrieNode<NodeType> node, String value) {
            if (params != null && node.isNamedWildcard()) {
                params.put(node.namedWildcard(), decoder.decode(value));
            }
        }
    }
}

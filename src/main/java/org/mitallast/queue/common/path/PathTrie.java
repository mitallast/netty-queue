package org.mitallast.queue.common.path;

import io.netty.handler.codec.http.QueryStringDecoder;
import org.mitallast.queue.common.strings.CharSequenceReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PathTrie<TrieType> {

    private final TrieNode<TrieType> root;
    private final char separator;
    private TrieType rootValue;

    public PathTrie() {
        this('/', "*");
    }

    public PathTrie(char separator, CharSequence wildcard) {
        this.separator = separator;
        root = new TrieNode<>(String.valueOf(separator), null, CharSequenceReference.of(wildcard));
    }

    public void insert(String path, TrieType value) {
        CharSequence[] parts = CharSequenceReference.splitStringToArray(path, separator);
        if (parts.length == 0) {
            rootValue = value;
            return;
        }
        int index = 0;
        // supports initial delimiter.
        if (parts.length > 0 && parts[0].length() == 0) {
            index = 1;
        }
        root.insert(parts, index, value);
    }

    public TrieType retrieve(CharSequence path, Map<String, List<String>> params) {
        if (path.length() == 0) {
            return rootValue;
        }
        CharSequence[] strings = CharSequenceReference.splitStringToArray(path, separator);
        if (strings.length == 0) {
            return rootValue;
        }
        int index = 0;
        // supports initial delimiter.
        if (strings.length > 0 && strings[0].length() == 0) {
            index = 1;
        }
        return root.retrieve(strings, index, params);
    }

    public class TrieNode<NodeType> {
        private final Map<CharSequenceReference, TrieNode<NodeType>> children;
        private final CharSequenceReference wildcard;
        private NodeType value;
        private String namedWildcard;

        public TrieNode(CharSequence key, NodeType value, CharSequenceReference wildcard) {
            this.wildcard = wildcard;
            this.value = value;
            this.children = new HashMap<>();
            if (isNamedWildcard(key)) {
                updateKeyWithNamedWildcard(key);
            } else {
                namedWildcard = null;
            }
        }

        public void updateKeyWithNamedWildcard(CharSequence key) {
            int len = key.length();
            for (int start = 0; start < len - 1; start++) {
                if (key.charAt(start) == '{') {
                    for (int end = len - 1; end > 0; end--) {
                        if (key.charAt(end) == '}') {
                            namedWildcard = key.subSequence(start + 1, end).toString();
                            return;
                        }
                    }
                }
            }
        }

        public synchronized void insert(CharSequence[] path, int index, NodeType value) {
            if (index >= path.length) {
                return;
            }

            CharSequence token = path[index];
            final CharSequenceReference key;
            if (isNamedWildcard(token)) {
                key = wildcard;
            } else {
                key = CharSequenceReference.of(token);
            }
            TrieNode<NodeType> node = children.get(key);
            if (node == null) {
                if (index == (path.length - 1)) {
                    node = new TrieNode<>(token, value, wildcard);
                } else {
                    node = new TrieNode<>(token, null, wildcard);
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

        private boolean isNamedWildcard(CharSequence key) {
            int len = key.length();
            for (int start = 0; start < len - 1; start++) {
                if (key.charAt(start) == '{') {
                    for (int end = len - 1; end > 0; end--) {
                        if (key.charAt(end) == '}') {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        private String namedWildcard() {
            return namedWildcard;
        }

        private boolean isNamedWildcard() {
            return namedWildcard != null;
        }

        public NodeType retrieve(CharSequence[] path, int index, Map<String, List<String>> params) {
            if (index >= path.length) {
                return null;
            }

            CharSequenceReference token = CharSequenceReference.of(path[index]);
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

        private void put(Map<String, List<String>> params, TrieNode<NodeType> node, CharSequence value) {
            if (params != null && node.isNamedWildcard()) {
                List<String> list = params.get(node.namedWildcard());
                if (list == null) {
                    list = new ArrayList<>(1);
                    params.put(node.namedWildcard(), list);
                }
                list.add(QueryStringDecoder.decodeComponent(value.toString()));
            }
        }
    }
}

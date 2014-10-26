package org.mitallast.queue.common.path;

import io.netty.handler.codec.http.QueryStringDecoder;
import org.mitallast.queue.common.Strings;

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

    public PathTrie(char separator, String wildcard) {
        this.separator = separator;
        root = new TrieNode<>(String.valueOf(separator), null, wildcard);
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

    public TrieType retrieve(String path, Map<String, List<String>> params) {
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

    public class TrieNode<NodeType> {
        private final Map<String, TrieNode<NodeType>> children;
        private final String wildcard;
        private NodeType value;
        private String namedWildcard;

        public TrieNode(String key, NodeType value, String wildcard) {
            this.wildcard = wildcard;
            this.value = value;
            this.children = new HashMap<>();
            if (isNamedWildcard(key)) {
                updateKeyWithNamedWildcard(key);
            } else {
                namedWildcard = null;
            }
        }

        public void updateKeyWithNamedWildcard(String key) {
            namedWildcard = key.substring(key.indexOf('{') + 1, key.indexOf('}'));
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

        private boolean isNamedWildcard(String key) {
            return key.indexOf('{') != -1 && key.indexOf('}') != -1;
        }

        private String namedWildcard() {
            return namedWildcard;
        }

        private boolean isNamedWildcard() {
            return namedWildcard != null;
        }

        public NodeType retrieve(String[] path, int index, Map<String, List<String>> params) {
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

        private void put(Map<String, List<String>> params, TrieNode<NodeType> node, String value) {
            if (params != null && node.isNamedWildcard()) {
                List<String> list = params.get(node.namedWildcard());
                if (list == null) {
                    list = new ArrayList<>(1);
                    params.put(node.namedWildcard(), list);
                }
                list.add(QueryStringDecoder.decodeComponent(value));
            }
        }
    }
}

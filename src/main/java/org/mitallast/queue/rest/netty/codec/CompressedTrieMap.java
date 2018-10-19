package org.mitallast.queue.rest.netty.codec;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.AsciiString;

import java.util.*;

public class CompressedTrieMap<T> {
    private final Node<T> root;

    private CompressedTrieMap(Node<T> root) {
        this.root = root;
    }

    public T find(String key) {
        return root.find(key, 0);
    }

    public void print() {
        root.prettyPrint(0, "", true);
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    private static class Node<T> {
        private final String key;
        private final T value;
        private final List<Node<T>> nodes;

        private Node(String key, T value, List<Node<T>> nodes) {
            this.key = key;
            this.value = value;
            this.nodes = nodes;
        }

        private void prettyPrint(int level, String prefix, boolean last) {
            System.out.print(prefix);
            if (level > 0) {
                if (last) {
                    System.out.print("└── ");
                } else {
                    System.out.print("├── ");
                }
            }
            System.out.print(key);
            System.out.print(" [");
            System.out.print(value);
            System.out.println("]");

            Iterator<Node<T>> iterator = nodes.iterator();
            while (iterator.hasNext()) {
                Node<T> child = iterator.next();
                boolean isLast = !iterator.hasNext();
                final String childPrefix;
                if (level > 0) {
                    if (last) {
                        childPrefix = prefix + "    ";
                    } else {
                        childPrefix = prefix + "├── ";
                    }
                } else childPrefix = prefix;
                child.prettyPrint(level + 1, childPrefix, isLast);
            }

            if (level == 0) {
                System.out.println();
            }
        }

        private T find(String key, int offset) {
            if (key.length() == offset) {
                return value;
            }

            int low = 0;
            int high = nodes.size() - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                Node<T> midVal = nodes.get(mid);
                int cmp = midVal.compare(key, offset);

                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return midVal.find(key, offset + midVal.key.length());
            }
            return null;
        }

        private int compare(String other, int offset) {
            int len1 = key.length();
            int len2 = other.length() - offset;
            int lim = Math.min(len1, len2);
            for (int k = 0; k < lim; k++) {
                char c1 = key.charAt(k);
                char c2 = other.charAt(k + offset);
                if (c1 != c2) {
                    return c1 - c2;
                }
            }
            return 0;
        }
    }

    private static class Entry<T> {
        private final String key;
        private final T value;

        private Entry(String key, T value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class Builder<T> {
        private final List<Entry<T>> entries = new ArrayList<>();

        public void add(String key, T value) {
            entries.add(new Entry<>(key, value));
        }

        public CompressedTrieMap<T> build() {
            return new CompressedTrieMap<>(buildNode());
        }

        Node<T> buildNode() {
            return buildNode("", entries, 0);
        }

        Node<T> buildNode(String key, List<Entry<T>> entries, int offset) {
            if (entries.isEmpty()) {
                return new Node<>(key, null, new ArrayList<>());
            } else if (entries.size() == 1) {
                Entry<T> entry = entries.get(0);
                return new Node<>(key, entry.value, new ArrayList<>());
            } else {
                List<Node<T>> nodes = new ArrayList<>();
                Entry<T> valueEntry = null;
                while (!entries.isEmpty()) {
                    List<Entry<T>> match = new ArrayList<>();
                    Iterator<Entry<T>> iterator = entries.iterator();
                    Entry<T> head = iterator.next();
                    iterator.remove();
                    match.add(head);
                    int max = head.key.length() - offset;
                    if (max == 0) { // value node
                        valueEntry = head;
                        continue;
                    }
                    while (iterator.hasNext()) {
                        Entry<T> entry = iterator.next();
                        int limit = Math.min(max, entry.key.length() - offset);
                        int matches = 0;
                        for (int i = 0; i < limit; i++) {
                            int pos = i + offset;
                            if (head.key.charAt(pos) != entry.key.charAt(pos)) {
                                break;
                            } else {
                                matches++;
                            }
                        }
                        if (matches > 0) {
                            max = Math.min(max, matches);
                            match.add(entry);
                            iterator.remove();
                        }
                    }
                    String prefix = head.key.substring(offset, offset + max);
                    Node<T> node = buildNode(prefix, match, offset + max);
                    nodes.add(node);
                }

                nodes.sort(Comparator.comparing(o -> o.key));

                if (valueEntry == null) {
                    return new Node<>(key, null, nodes);
                } else {
                    return new Node<>(key, valueEntry.value, nodes);
                }
            }
        }
    }
}

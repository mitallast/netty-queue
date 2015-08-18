package org.mitallast.queue.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class Immutable {

    public static <K, V> ImmutableMap<K, V> compose(ImmutableMap<K, V> map, K k, V v) {
        return ImmutableMap.<K, V>builder()
            .putAll(map)
            .put(k, v)
            .build();
    }

    public static <K, V> ImmutableMap<K, V> subtract(ImmutableMap<K, V> map, K k) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.<K, V>builder();
        map.entrySet().stream()
            .filter(kvEntry -> !kvEntry.getKey().equals(k))
            .forEach(builder::put);
        return builder.build();
    }

    public static <V> ImmutableList<V> compose(Iterable<V> list, V v) {
        return ImmutableList.<V>builder()
            .addAll(list)
            .add(v)
            .build();
    }

    public static <V> ImmutableList<V> subtract(Iterable<V> list, V v) {
        ImmutableList.Builder<V> builder = ImmutableList.<V>builder();
        for (V item : list) {
            if (!item.equals(v)) {
                builder.add(item);
            }
        }
        return builder.build();
    }
}
